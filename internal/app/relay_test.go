package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

func TestQuota(t *testing.T) {
	tests := []struct {
		batch, maxInflight, queued, want int
	}{
		{500, 1000, 0, 500},   // batch is the binding limit
		{500, 1000, 800, 200}, // only 200 slots left under the cap
		{500, 1000, 1000, 0},  // at the cap: skip
		{500, 1000, 1200, 0},  // over the cap (cap lowered at runtime): never negative
		{10, 1000, 995, 5},    // min of both
		{0, 1000, 0, 0},       // degenerate batch
		{500, 0, 0, 0},        // degenerate cap
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("b%d_c%d_q%d", tc.batch, tc.maxInflight, tc.queued), func(t *testing.T) {
			if got := Quota(tc.batch, tc.maxInflight, tc.queued); got != tc.want {
				t.Fatalf("Quota = %d, want %d", got, tc.want)
			}
		})
	}
}

type relayFixture struct {
	repo    *memRepo
	pub     *recPublisher
	events  *recBroadcaster
	metrics *recMetrics
	clock   *fakeClock
	logs    *logCapture
	relay   *Relay
}

func newRelayFixture(maxInflight, batch int) relayFixture {
	f := relayFixture{repo: newRepo(), pub: &recPublisher{}, events: &recBroadcaster{}, metrics: newMetrics(),
		clock: newClock(), logs: &logCapture{}}
	f.relay = NewRelay(RelayDeps{
		Repo: f.repo, Publisher: f.pub, Events: f.events, Metrics: f.metrics, Clock: f.clock,
		IDs: &seqIDs{}, Log: f.logs.logger(), MaxInflight: maxInflight, BatchSize: batch,
	})
	return f
}

// due adds a pending row due now.
func (f relayFixture) due(id string, ch domain.Channel, p domain.Priority, age time.Duration) {
	n := queuedNotification(id, ch, f.clock.Now().Add(-age))
	n.Status, n.Priority = domain.StatusPending, p
	f.repo.put(n)
}

func TestRelayPublishesDueRowsInPriorityOrder(t *testing.T) {
	f := newRelayFixture(1000, 500)
	f.due("low-old", domain.ChannelSMS, domain.PriorityLow, time.Minute)
	f.due("high", domain.ChannelSMS, domain.PriorityHigh, 0)
	f.due("normal", domain.ChannelSMS, domain.PriorityNormal, time.Second)
	future := queuedNotification("later", domain.ChannelSMS, f.clock.Now().Add(time.Hour))
	future.Status = domain.StatusScheduled
	f.repo.put(future)

	if err := f.relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, m := range f.pub.published() {
		order = append(order, m.NotificationID)
	}
	if strings.Join(order, ",") != "high,normal,low-old" {
		t.Fatalf("publish order %v, want high,normal,low-old (scheduled row not due)", order)
	}
	m := f.pub.published()[0]
	if m.Channel != domain.ChannelSMS || m.Priority != domain.PriorityHigh || m.CorrelationID != "c-high" {
		t.Fatalf("message fields: %+v", m)
	}
	for _, id := range []string{"high", "normal", "low-old"} {
		if f.repo.get(id).Status != domain.StatusQueued {
			t.Errorf("%s not queued", id)
		}
		if got := f.events.statuses(id); len(got) != 1 || got[0] != domain.StatusQueued {
			t.Errorf("%s events %v, want one queued", id, got)
		}
	}
	if f.repo.get("later").Status != domain.StatusScheduled {
		t.Fatal("scheduled row relayed before its time")
	}
	if f.metrics.get("relay_published:sms") != 3 {
		t.Fatalf("relay_published:sms = %d", f.metrics.get("relay_published:sms"))
	}
}

func TestRelayBackpressureSkipsOnlyTheFullChannel(t *testing.T) {
	f := newRelayFixture(2, 500) // cap of 2 queued per channel
	for i := range 2 {
		f.repo.put(queuedNotification(fmt.Sprintf("q%d", i), domain.ChannelSMS, f.clock.Now()))
	}
	f.due("sms-waiting", domain.ChannelSMS, domain.PriorityHigh, 0)
	f.due("email-due", domain.ChannelEmail, domain.PriorityNormal, 0)

	if err := f.relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.repo.get("sms-waiting").Status != domain.StatusPending {
		t.Fatal("sms is at the cap: its backlog must stay in the database")
	}
	if f.repo.get("email-due").Status != domain.StatusQueued {
		t.Fatal("email must not be blocked by the sms cap")
	}
	if f.metrics.get("relay_skipped:sms") != 1 || f.metrics.get("relay_skipped:email") != 0 {
		t.Fatalf("skip metric sms=%d email=%d", f.metrics.get("relay_skipped:sms"), f.metrics.get("relay_skipped:email"))
	}
}

func TestRelayPartialQuota(t *testing.T) {
	f := newRelayFixture(3, 500)
	f.repo.put(queuedNotification("already", domain.ChannelPush, f.clock.Now()))
	for i := range 5 {
		f.due(fmt.Sprintf("p%d", i), domain.ChannelPush, domain.PriorityNormal, time.Duration(i)*time.Second)
	}
	if err := f.relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(f.pub.published()); n != 2 {
		t.Fatalf("published %d, want 2 (cap 3 minus 1 queued)", n)
	}
}

func TestRelayLockedChannelIsSkippedWithoutMetric(t *testing.T) {
	f := newRelayFixture(1000, 500)
	f.repo.locked[domain.ChannelSMS] = true
	f.due("sms", domain.ChannelSMS, domain.PriorityNormal, 0)
	if err := f.relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.repo.get("sms").Status != domain.StatusPending || len(f.pub.published()) != 0 {
		t.Fatal("another relay holds the channel: nothing to do this tick")
	}
	if f.metrics.get("relay_skipped:sms") != 0 {
		t.Fatal("a held lock is not backpressure")
	}
}

func TestRelayPublishFailureRollsBack(t *testing.T) {
	f := newRelayFixture(1000, 500)
	f.pub.err = errors.New("broker nack")
	f.due("sms", domain.ChannelSMS, domain.PriorityNormal, 0)
	f.due("email", domain.ChannelEmail, domain.PriorityNormal, 0)

	err := f.relay.Tick(context.Background())
	if err == nil || !strings.Contains(err.Error(), "broker nack") {
		t.Fatalf("tick must report the failure, got %v", err)
	}
	for _, id := range []string{"sms", "email"} {
		if f.repo.get(id).Status != domain.StatusPending {
			t.Errorf("%s must stay due after a rolled-back publish", id)
		}
		if len(f.events.statuses(id)) != 0 {
			t.Errorf("%s: no event for a rolled-back transition", id)
		}
	}
	// Rows logged on failure carry their own correlation id; the tick carries a tick_id.
	logs := f.logs.String()
	if !strings.Contains(logs, `"correlation_id":"c-sms"`) || !strings.Contains(logs, `"tick_id"`) {
		t.Fatalf("logs lack correlation or tick ids:\n%s", logs)
	}
}
