package app

import (
	"context"
	"errors"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

type cancelFixture struct {
	repo    *memRepo
	batches *memBatchRepo
	events  *recBroadcaster
	uc      *Canceller
}

func newCancelFixture() cancelFixture {
	f := cancelFixture{repo: newRepo(), batches: newBatchRepo(), events: &recBroadcaster{}}
	f.uc = NewCanceller(CancellerDeps{Repo: f.repo, Batches: f.batches, Events: f.events, Clock: newClock(), Log: quietLog})
	return f
}

func TestCancelEachCancellableStatus(t *testing.T) {
	for _, st := range domain.CancellableFrom {
		t.Run(string(st), func(t *testing.T) {
			f := newCancelFixture()
			n := queuedNotification("n1", domain.ChannelSMS, newClock().Now())
			n.Status = st
			f.repo.put(n)

			got, err := f.uc.Cancel(context.Background(), "n1")
			if err != nil || got.Status != domain.StatusCancelled || got.CancelledAt == nil {
				t.Fatalf("cancel from %s: %+v %v", st, got, err)
			}
			ev := f.events.events
			if len(ev) != 1 || ev[0].To != domain.StatusCancelled || ev[0].From == nil || *ev[0].From != st {
				t.Fatalf("event %+v, want %s → cancelled", ev, st)
			}
		})
	}
}

func TestCancelRejectsWorkInProgressOrDone(t *testing.T) {
	for _, st := range []domain.Status{domain.StatusSending, domain.StatusSent, domain.StatusFailed} {
		t.Run(string(st), func(t *testing.T) {
			f := newCancelFixture()
			n := queuedNotification("n1", domain.ChannelSMS, newClock().Now())
			n.Status = st
			f.repo.put(n)

			_, err := f.uc.Cancel(context.Background(), "n1")
			var nc *domain.NotCancellableError
			if !errors.As(err, &nc) || nc.Current != st {
				t.Fatalf("err %v, want NotCancellableError{%s} (US5-AS2)", err, st)
			}
			if f.repo.get("n1").Status != st || len(f.events.events) != 0 {
				t.Fatal("a rejected cancel must change nothing")
			}
		})
	}
}

func TestCancelIsIdempotentAndUnknownIsNotFound(t *testing.T) {
	f := newCancelFixture()
	n := queuedNotification("n1", domain.ChannelSMS, newClock().Now())
	n.Status = domain.StatusCancelled
	f.repo.put(n)
	got, err := f.uc.Cancel(context.Background(), "n1")
	if err != nil || got.Status != domain.StatusCancelled || len(f.events.events) != 0 {
		t.Fatalf("re-cancel: %+v %v, want success with no change (US5-AS6)", got, err)
	}
	if _, err := f.uc.Cancel(context.Background(), "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
}

func TestCancelBatchCounts(t *testing.T) {
	f := newCancelFixture()
	bid := "batch-1"
	f.batches.batches[bid] = domain.Batch{ID: bid}
	for i, st := range []domain.Status{domain.StatusPending, domain.StatusScheduled, domain.StatusQueued,
		domain.StatusSending, domain.StatusSent, domain.StatusCancelled} {
		n := queuedNotification(string(rune('a'+i)), domain.ChannelSMS, newClock().Now())
		n.Status, n.BatchID = st, &bid
		f.batches.rows[n.ID] = n
	}

	res, err := f.uc.CancelBatch(context.Background(), bid)
	if err != nil {
		t.Fatal(err)
	}
	if res.Cancelled != 3 || res.AlreadyCancelled != 1 || res.NotCancellable != 2 {
		t.Fatalf("counts %+v, want cancelled 3, already 1, not cancellable 2 (FR-015)", res)
	}
	if len(f.events.events) != 3 {
		t.Fatalf("events %d, want one per cancelled item", len(f.events.events))
	}
	if _, err := f.uc.CancelBatch(context.Background(), "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown batch: %v", err)
	}
}
