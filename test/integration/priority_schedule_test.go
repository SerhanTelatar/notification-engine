//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

func TestHighPriorityOvertakesALowBacklog(t *testing.T) {
	const (
		backlog     = 300
		concurrency = 5
		workers     = 1
	)
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: 202, Delay: 20 * time.Millisecond}
	})
	insertBurst(t, domain.ChannelSMS, "+905551234567", backlog)
	if _, err := env.Pool.Exec(context.Background(), `UPDATE notifications SET priority = 1`); err != nil {
		t.Fatal(err)
	}
	cfg := defaultPipeline(mock.SendURL())
	cfg.concurrency = concurrency
	// The count below starts when the high item is created, but it only reaches the queue on the
	// next relay tick. Keep that gap tiny so the count measures prefetch, which is what SC-007
	// bounds, not relay latency.
	cfg.relayInterval = 5 * time.Millisecond
	p := startPipeline(t, cfg)

	// Let the low backlog land in RabbitMQ and start draining.
	waitFor(t, 10*time.Second, "low backlog draining", func() bool { return len(mock.Received()) >= 20 })
	before := len(mock.Received())
	high := p.create(t, app.CreateRequest{Priority: "high", CorrelationID: "vip"})
	waitFor(t, 10*time.Second, "high sent", statusIs(t, high.ID, "sent"))

	pos := -1
	for i, r := range mock.Received() {
		if r.IdempotencyKey == high.ID {
			pos = i
		}
	}
	overtaken := pos - before // low items sent after the high one was created, before it
	t.Logf("high created after %d sends, delivered at position %d: %d low items went first", before, pos, overtaken)
	// SC-007: only messages consumers had already taken off the queue may go first.
	if limit := concurrency*workers + 2; overtaken > limit {
		t.Fatalf("%d low items were sent before the high one; the prefetch bound allows %d", overtaken, limit)
	}
	if remaining := countStatus(t, "queued") + countStatus(t, "pending"); remaining < backlog/2 {
		t.Fatalf("only %d low items still waiting: the backlog drained too fast to test priority", remaining)
	}
}

func TestScheduledDeliveryIsNeverEarly(t *testing.T) {
	p, mock := setupDelivery(t, nil, nil)
	at := time.Now().Add(3 * time.Second)
	n := p.create(t, app.CreateRequest{ScheduledAt: &at})
	if n.Status != domain.StatusScheduled {
		t.Fatalf("status %s, want scheduled", n.Status)
	}
	waitFor(t, 10*time.Second, "sent", statusIs(t, n.ID, "sent"))
	got := mock.Received()
	if len(got) != 1 {
		t.Fatalf("provider calls %d", len(got))
	}
	delay := got[0].At.Sub(at)
	t.Logf("scheduled for %s, delivered %v after", at.Format(time.TimeOnly), delay.Round(time.Millisecond))
	if delay < 0 {
		t.Fatalf("delivered %v before its scheduled time (FR-018)", -delay)
	}
	if delay > 5*time.Second {
		t.Fatalf("delivered %v after its scheduled time, want within 5 s (SC-008)", delay)
	}
}

func TestPastScheduleMeansNow(t *testing.T) {
	p, mock := setupDelivery(t, nil, nil)
	past := time.Now().Add(-time.Hour)
	n := p.create(t, app.CreateRequest{ScheduledAt: &past})
	if n.Status != domain.StatusPending || n.ScheduledAt != nil {
		t.Fatalf("status %s scheduled_at %v: a past schedule is delivered now", n.Status, n.ScheduledAt)
	}
	waitFor(t, 5*time.Second, "sent", statusIs(t, n.ID, "sent"))
	if len(mock.Received()) != 1 {
		t.Fatal("provider must receive it")
	}
}
