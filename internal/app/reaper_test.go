package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

type reaperFixture struct {
	repo    *memRepo
	pub     *recPublisher
	events  *recBroadcaster
	metrics *recMetrics
	clock   *fakeClock
	uc      *Reaper
}

func newReaperFixture() reaperFixture {
	f := reaperFixture{repo: newRepo(), pub: &recPublisher{}, events: &recBroadcaster{}, metrics: newMetrics(), clock: newClock()}
	f.uc = NewReaper(ReaperDeps{
		Repo: f.repo, Publisher: f.pub, Events: f.events, Metrics: f.metrics, Clock: f.clock, Log: quietLog,
		SendingTimeout: 2 * time.Minute, QueuedTimeout: 15 * time.Minute, Limit: 100,
	})
	return f
}

// stale adds a row in status st whose last update was age ago.
func (f reaperFixture) stale(id string, st domain.Status, age time.Duration, attempts int) {
	n := queuedNotification(id, domain.ChannelSMS, f.clock.Now().Add(-age))
	n.Status, n.Attempts = st, attempts
	f.repo.put(n)
}

func TestReaperRecoversStaleSending(t *testing.T) {
	f := newReaperFixture()
	f.stale("abandoned", domain.StatusSending, 10*time.Minute, 1)
	f.stale("last-chance", domain.StatusSending, 10*time.Minute, 4) // max_attempts 5
	f.stale("in-flight", domain.StatusSending, 30*time.Second, 0)

	if err := f.uc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}

	n := f.repo.get("abandoned")
	if n.Status != domain.StatusRetrying || n.Attempts != 2 || !n.NextAttemptAt.Equal(f.clock.Now()) {
		t.Fatalf("abandoned row %+v: want retrying now with attempts+1 (the call may have reached the provider)", n)
	}
	a := f.repo.attemptsOf("abandoned")
	if len(a) != 1 || a[0].Outcome != domain.OutcomeAbandoned || a[0].AttemptNo != 2 || a[0].Error == nil {
		t.Fatalf("abandoned attempt %+v", a)
	}

	if n := f.repo.get("last-chance"); n.Status != domain.StatusFailed || n.Attempts != 5 || n.FailedAt == nil {
		t.Fatalf("exhausted row %+v: want failed", n)
	}
	if f.repo.get("in-flight").Status != domain.StatusSending {
		t.Fatal("a send younger than REAPER_SENDING_TIMEOUT must be left alone")
	}
	if f.metrics.get("reaped:sending") != 2 || f.metrics.get("dead:sms:exhausted") != 1 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}
	if got := f.events.statuses("abandoned"); len(got) != 1 || got[0] != domain.StatusRetrying {
		t.Fatalf("events %v", got)
	}
}

func TestReaperRepublishesStaleQueued(t *testing.T) {
	f := newReaperFixture()
	f.stale("lost", domain.StatusQueued, 20*time.Minute, 0)
	f.stale("waiting", domain.StatusQueued, time.Minute, 0)

	if err := f.uc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	msgs := f.pub.published()
	if len(msgs) != 1 || msgs[0].NotificationID != "lost" || msgs[0].CorrelationID != "c-lost" {
		t.Fatalf("republished %+v, want only the stale row", msgs)
	}
	if n := f.repo.get("lost"); n.Status != domain.StatusQueued || !n.UpdatedAt.Equal(f.clock.Now()) {
		t.Fatalf("republished row must stay queued with a fresh updated_at: %+v", n)
	}
	if f.metrics.get("reaped:queued") != 1 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}
}

func TestReaperReportsPublishFailure(t *testing.T) {
	f := newReaperFixture()
	f.pub.err = errors.New("broker down")
	f.stale("lost", domain.StatusQueued, 20*time.Minute, 0)
	if err := f.uc.Tick(context.Background()); err == nil {
		t.Fatal("a failed republish must be reported")
	}
	if f.metrics.get("reaped:queued") != 0 {
		t.Fatal("nothing was recovered")
	}
}

func TestReaperNothingToDo(t *testing.T) {
	f := newReaperFixture()
	if err := f.uc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.pub.published()) != 0 || f.metrics.get("reaped:sending") != 0 {
		t.Fatal("an idle tick must do nothing")
	}
}
