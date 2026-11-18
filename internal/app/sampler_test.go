package app

import (
	"context"
	"errors"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

type backlogRepo struct {
	*memRepo
	counts []ports.BacklogCount
}

func (r *backlogRepo) BacklogCounts(context.Context) ([]ports.BacklogCount, error) {
	return r.counts, nil
}

type fakeQueues struct {
	depths map[string]int
	err    error
}

func (q *fakeQueues) Depths(context.Context) (map[string]int, error) { return q.depths, q.err }

func backlogValue(c []ports.BacklogCount, ch domain.Channel, p domain.Priority, st domain.Status) (int, bool) {
	for _, b := range c {
		if b.Channel == ch && b.Priority == p && b.Status == st {
			return b.Count, true
		}
	}
	return 0, false
}

func TestSamplerReportsBacklogAndResetsVanishedCombinations(t *testing.T) {
	repo := &backlogRepo{memRepo: newRepo(), counts: []ports.BacklogCount{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusQueued, Count: 7},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityLow, Status: domain.StatusPending, Count: 3},
	}}
	m := newMetrics()
	queues := &fakeQueues{depths: map[string]int{"notifications.sms": 7, "notifications.dlq": 1}}
	s := NewSampler(SamplerDeps{Repo: repo, Queues: queues, Metrics: m, Log: quietLog})

	if err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := backlogValue(m.backlog, domain.ChannelSMS, domain.PriorityHigh, domain.StatusQueued); !ok || v != 7 {
		t.Fatalf("sms/high/queued = %d (%v), want 7", v, ok)
	}
	if m.depths["notifications.sms"] != 7 || m.depths["notifications.dlq"] != 1 {
		t.Fatalf("queue depths %v", m.depths)
	}

	// The sms backlog drained: that combination must drop to 0, not linger at 7.
	repo.counts = repo.counts[1:]
	if err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, ok := backlogValue(m.backlog, domain.ChannelSMS, domain.PriorityHigh, domain.StatusQueued); !ok || v != 0 {
		t.Fatalf("vanished combination reported as %d (%v), want an explicit 0", v, ok)
	}
	if v, _ := backlogValue(m.backlog, domain.ChannelEmail, domain.PriorityLow, domain.StatusPending); v != 3 {
		t.Fatalf("email/low/pending = %d, want 3", v)
	}
}

func TestSamplerBrokerErrorDoesNotStopDBGauges(t *testing.T) {
	repo := &backlogRepo{memRepo: newRepo(), counts: []ports.BacklogCount{
		{Channel: domain.ChannelPush, Priority: domain.PriorityNormal, Status: domain.StatusRetrying, Count: 2},
	}}
	m := newMetrics()
	s := NewSampler(SamplerDeps{Repo: repo, Queues: &fakeQueues{err: errors.New("channel closed")}, Metrics: m, Log: quietLog})

	err := s.Tick(context.Background())
	if err == nil {
		t.Fatal("the broker error must be reported")
	}
	if v, ok := backlogValue(m.backlog, domain.ChannelPush, domain.PriorityNormal, domain.StatusRetrying); !ok || v != 2 {
		t.Fatalf("backlog gauge %d (%v): DB-based gauges must still be set", v, ok)
	}
}
