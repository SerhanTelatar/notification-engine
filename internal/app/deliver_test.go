package app

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

type deliverFixture struct {
	repo     *memRepo
	provider *scriptProvider
	limiter  *fakeLimiter
	breaker  *fakeBreaker
	events   *recBroadcaster
	metrics  *recMetrics
	clock    *fakeClock
	trace    *trace
	uc       *Deliverer
}

func newDeliverFixture() deliverFixture {
	tr := &trace{}
	f := deliverFixture{
		repo: newRepo(), provider: &scriptProvider{trace: tr}, limiter: &fakeLimiter{trace: tr},
		breaker: &fakeBreaker{}, events: &recBroadcaster{}, metrics: newMetrics(), clock: newClock(), trace: tr,
	}
	f.repo.trace = tr
	f.uc = NewDeliverer(DelivererDeps{
		Repo: f.repo, Provider: f.provider, Breaker: f.breaker, Limiter: f.limiter, Events: f.events,
		Metrics: f.metrics, Clock: f.clock, Log: quietLog,
		BackoffBase: time.Second, BackoffMax: 5 * time.Minute, LimiterMaxWait: 50 * time.Millisecond,
		Rand: func(n int64) int64 { return n - 1 }, // top of the jitter range: deterministic
	})
	return f
}

// queue adds a queued row and returns its message.
func (f deliverFixture) queue(id string, mut func(*domain.Notification)) ports.Message {
	n := queuedNotification(id, domain.ChannelSMS, f.clock.Now())
	if mut != nil {
		mut(n)
	}
	f.repo.put(n)
	return ports.Message{NotificationID: id, Channel: n.Channel, Priority: n.Priority, CorrelationID: n.CorrelationID}
}

func TestDeliverSuccess(t *testing.T) {
	f := newDeliverFixture()
	msg := f.queue("n1", nil)

	if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionAck {
		t.Fatalf("decision %s, want ack", d)
	}
	n := f.repo.get("n1")
	if n.Status != domain.StatusSent || n.Attempts != 1 || n.ProviderMessageID == nil || *n.ProviderMessageID != "msg-1" || n.SentAt == nil {
		t.Fatalf("row after success: %+v", n)
	}
	if got := strings.Join(f.trace.list(), ","); got != "acquire,start,send,apply:sent" {
		t.Fatalf("call order %s: the permit must come before the queued→sending claim", got)
	}
	a := f.repo.attemptsOf("n1")
	if len(a) != 1 || a[0].Outcome != domain.OutcomeSuccess || a[0].AttemptNo != 1 || a[0].HTTPStatus == nil || *a[0].HTTPStatus != 202 {
		t.Fatalf("attempt row %+v", a)
	}
	req := f.provider.calls[0]
	if req.NotificationID != "n1" || req.CorrelationID != "c-n1" || req.To != "+905551234567" || req.Content != "hi" {
		t.Fatalf("provider request %+v", req)
	}
	if got := f.events.statuses("n1"); !slices.Equal(got, []domain.Status{domain.StatusSending, domain.StatusSent}) {
		t.Fatalf("events %v", got)
	}
	if f.metrics.get("delivered:sms:success") != 1 || f.metrics.get("e2e:sms") != 1 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}
}

func TestDeliverDropsWhenGuardMatchesNothing(t *testing.T) {
	for _, st := range []domain.Status{domain.StatusCancelled, domain.StatusSent, domain.StatusFailed, domain.StatusSending, domain.StatusRetrying} {
		t.Run(string(st), func(t *testing.T) {
			f := newDeliverFixture()
			msg := f.queue("n1", func(n *domain.Notification) { n.Status = st })
			if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionAck {
				t.Fatalf("decision %s, want ack (drop)", d)
			}
			if f.provider.callCount() != 0 {
				t.Fatalf("a %s notification must never reach the provider (FR-014, FR-026)", st)
			}
			if f.repo.get("n1").Status != st {
				t.Fatal("status must not change")
			}
		})
	}
	t.Run("unknown id", func(t *testing.T) {
		f := newDeliverFixture()
		if d := f.uc.Handle(context.Background(), ports.Message{NotificationID: "missing", Channel: domain.ChannelSMS}); d != ports.DecisionAck {
			t.Fatalf("decision %s", d)
		}
	})
}

func TestDeliverTransientRetriesWithBackoff(t *testing.T) {
	tests := []struct {
		name       string
		attempts   int // before this delivery
		result     ports.ProviderResult
		wantDelay  time.Duration
		wantStatus domain.Status
		wantDec    ports.Decision
		wantMetric string
	}{
		{"first 503 retries after the attempt-1 ceiling", 0, httpResult(503, domain.OutcomeTransient, "provider returned HTTP 503"),
			time.Second, domain.StatusRetrying, ports.DecisionAck, "retried:sms"},
		{"third failure doubles twice", 2, httpResult(503, domain.OutcomeTransient, "HTTP 503"),
			4 * time.Second, domain.StatusRetrying, ports.DecisionAck, "retried:sms"},
		{"Retry-After is a floor", 0, func() ports.ProviderResult {
			r := httpResult(429, domain.OutcomeTransient, "HTTP 429")
			r.RetryAfter = 30 * time.Second
			return r
		}(), 30 * time.Second, domain.StatusRetrying, ports.DecisionAck, "retried:sms"},
		{"timeout is transient", 0, ports.ProviderResult{Outcome: domain.OutcomeTransient, Error: "provider timed out after 5s"},
			time.Second, domain.StatusRetrying, ports.DecisionAck, "retried:sms"},
		{"fifth failure exhausts max_attempts", 4, httpResult(503, domain.OutcomeTransient, "HTTP 503"),
			0, domain.StatusFailed, ports.DecisionDeadLetter, "dead:sms:exhausted"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliverFixture()
			f.provider.results = []ports.ProviderResult{tc.result}
			msg := f.queue("n1", func(n *domain.Notification) { n.Attempts = tc.attempts })

			if d := f.uc.Handle(context.Background(), msg); d != tc.wantDec {
				t.Fatalf("decision %s, want %s", d, tc.wantDec)
			}
			n := f.repo.get("n1")
			if n.Status != tc.wantStatus || n.Attempts != tc.attempts+1 || n.LastError == nil || *n.LastError != tc.result.Error {
				t.Fatalf("row %+v", n)
			}
			if tc.wantStatus == domain.StatusRetrying {
				if got := n.NextAttemptAt.Sub(f.clock.Now()); got != tc.wantDelay {
					t.Fatalf("next attempt in %v, want %v", got, tc.wantDelay)
				}
			} else if n.FailedAt == nil {
				t.Fatal("failed_at must be set")
			}
			if a := f.repo.attemptsOf("n1"); len(a) != 1 || a[0].Outcome != domain.OutcomeTransient || a[0].Error == nil {
				t.Fatalf("attempt %+v", a)
			}
			if f.metrics.get(tc.wantMetric) != 1 || f.metrics.get("delivered:sms:transient") != 1 {
				t.Fatalf("metrics %v", f.metrics.counts)
			}
		})
	}
}

func TestDeliverPermanentFailsImmediately(t *testing.T) {
	f := newDeliverFixture()
	f.provider.results = []ports.ProviderResult{httpResult(400, domain.OutcomePermanent, "provider returned HTTP 400: bad recipient")}
	msg := f.queue("n1", nil)

	if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionDeadLetter {
		t.Fatalf("decision %s, want dead_letter", d)
	}
	n := f.repo.get("n1")
	if n.Status != domain.StatusFailed || n.Attempts != 1 {
		t.Fatalf("row %+v: a permanent failure must not be retried (FR-020)", n)
	}
	if f.metrics.get("dead:sms:permanent") != 1 || f.metrics.get("retried:sms") != 0 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}
}

func TestDeliverLimiterWaitIsCapped(t *testing.T) {
	t.Run("no permit within LIMITER_MAX_WAIT releases the message", func(t *testing.T) {
		f := newDeliverFixture()
		f.limiter.block = true
		msg := f.queue("n1", nil)
		start := time.Now()
		if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionRequeue {
			t.Fatalf("decision %s, want requeue", d)
		}
		if time.Since(start) > time.Second {
			t.Fatal("the wait must be bounded by LIMITER_MAX_WAIT")
		}
		if f.repo.get("n1").Status != domain.StatusQueued || f.provider.callCount() != 0 {
			t.Fatal("row must stay queued and the provider untouched")
		}
		if f.metrics.get("limiter_timeout:sms") != 1 {
			t.Fatalf("metrics %v", f.metrics.counts)
		}
	})
	t.Run("limiter errors are retried, then released (fail closed)", func(t *testing.T) {
		f := newDeliverFixture()
		f.limiter.err = errors.New("redis: connection refused")
		msg := f.queue("n1", nil)
		if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionRequeue {
			t.Fatalf("decision %s", d)
		}
		if f.limiter.callCount() < 2 || f.provider.callCount() != 0 {
			t.Fatalf("limiter calls %d (want retries), provider calls %d (want 0)", f.limiter.callCount(), f.provider.callCount())
		}
	})
	t.Run("shutdown during the wait releases at once", func(t *testing.T) {
		f := newDeliverFixture()
		f.limiter.block = true
		msg := f.queue("n1", nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if d := f.uc.Handle(ctx, msg); d != ports.DecisionRequeue {
			t.Fatalf("decision %s", d)
		}
		if f.metrics.get("limiter_timeout:sms") != 0 {
			t.Fatal("a shutdown is not a limiter timeout")
		}
	})
}

func TestDeliverFinishesInFlightWorkOnShutdown(t *testing.T) {
	// Cancelled after the permit: the claimed send must still complete and be recorded.
	f := newDeliverFixture()
	msg := f.queue("n1", nil)
	ctx, cancel := context.WithCancel(context.Background())
	f.uc.d.Repo = &cancellingRepo{memRepo: f.repo, cancel: cancel}
	if d := f.uc.Handle(ctx, msg); d != ports.DecisionAck {
		t.Fatalf("decision %s", d)
	}
	if n := f.repo.get("n1"); n.Status != domain.StatusSent {
		t.Fatalf("in-flight send abandoned on shutdown: %s", n.Status)
	}
}

// cancellingRepo cancels the worker ctx right after the claim, like a SIGTERM mid-flight.
type cancellingRepo struct {
	*memRepo
	cancel context.CancelFunc
}

func (r *cancellingRepo) StartSending(ctx context.Context, id string, at time.Time) (*domain.Notification, bool, error) {
	n, ok, err := r.memRepo.StartSending(ctx, id, at)
	r.cancel()
	return n, ok, err
}

func TestDeliverStoreFailures(t *testing.T) {
	t.Run("claim error requeues without calling the provider", func(t *testing.T) {
		f := newDeliverFixture()
		f.repo.startErr = errors.New("db down")
		msg := f.queue("n1", nil)
		if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionRequeue || f.provider.callCount() != 0 {
			t.Fatalf("decision %s calls %d", d, f.provider.callCount())
		}
	})
	t.Run("outcome error acks: the reaper recovers the sending row", func(t *testing.T) {
		f := newDeliverFixture()
		f.repo.applyErr = errors.New("db down")
		msg := f.queue("n1", nil)
		if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionAck {
			t.Fatalf("decision %s: redelivering would only be dropped by the guard", d)
		}
		if f.repo.get("n1").Status != domain.StatusSending || f.provider.callCount() != 1 {
			t.Fatal("row stays sending for the reaper; exactly one provider call")
		}
	})
}

// slowClaimRepo makes the queued→sending claim take a while, like a loaded database.
type slowClaimRepo struct {
	*memRepo
	delay time.Duration
}

func (r *slowClaimRepo) StartSending(ctx context.Context, id string, at time.Time) (*domain.Notification, bool, error) {
	time.Sleep(r.delay)
	return r.memRepo.StartSending(ctx, id, at)
}

func TestDeliverPacesTheCallOnTheSlot(t *testing.T) {
	f := newDeliverFixture()
	f.uc.d.SendOffset = 60 * time.Millisecond
	var sentAt time.Time
	f.provider.trace = nil
	slotTaken := time.Now()
	f.uc.d.Provider = providerFunc(func(ctx context.Context, req ports.ProviderRequest) ports.ProviderResult {
		sentAt = time.Now()
		return f.provider.Send(ctx, req)
	})
	if d := f.uc.Handle(context.Background(), f.queue("n1", nil)); d != ports.DecisionAck {
		t.Fatalf("decision %s", d)
	}
	if early := sentAt.Sub(slotTaken); early < 60*time.Millisecond {
		t.Fatalf("provider called %v after the slot, want >= the 60ms send offset", early)
	}
	if f.limiter.callCount() != 1 {
		t.Fatalf("a claim within the offset needs one slot, got %d", f.limiter.callCount())
	}
}

func TestDeliverReservesAFreshSlotWhenTheClaimOverrunsTheOffset(t *testing.T) {
	f := newDeliverFixture()
	f.uc.d.SendOffset = 20 * time.Millisecond
	f.uc.d.Repo = &slowClaimRepo{memRepo: f.repo, delay: 60 * time.Millisecond}
	if d := f.uc.Handle(context.Background(), f.queue("n1", nil)); d != ports.DecisionAck {
		t.Fatalf("decision %s", d)
	}
	if f.limiter.callCount() != 2 {
		t.Fatalf("limiter calls %d: a stale slot must be replaced, never sent on late", f.limiter.callCount())
	}
	if f.repo.get("n1").Status != domain.StatusSent {
		t.Fatal("notification must still be delivered")
	}
}

func TestDeliverReleasesTheClaimWhenNoFreshSlotComes(t *testing.T) {
	f := newDeliverFixture()
	f.uc.d.SendOffset = 10 * time.Millisecond
	slow := &slowClaimRepo{memRepo: f.repo, delay: 40 * time.Millisecond}
	f.uc.d.Repo = slow
	msg := f.queue("n1", nil)
	// First slot is granted; while the claim runs, the limiter stops granting.
	f.uc.d.Limiter = &firstThenBlockLimiter{}
	if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionAck {
		t.Fatalf("decision %s", d)
	}
	n := f.repo.get("n1")
	if n.Status != domain.StatusRetrying || n.Attempts != 0 || f.provider.callCount() != 0 {
		t.Fatalf("row %s attempts %d calls %d: release to retrying without an attempt or a send",
			n.Status, n.Attempts, f.provider.callCount())
	}
	if f.metrics.get("limiter_timeout:sms") != 1 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}
}

type providerFunc func(context.Context, ports.ProviderRequest) ports.ProviderResult

func (f providerFunc) Send(ctx context.Context, r ports.ProviderRequest) ports.ProviderResult {
	return f(ctx, r)
}

// firstThenBlockLimiter grants one slot, then blocks until the caller gives up.
type firstThenBlockLimiter struct{ granted bool }

func (l *firstThenBlockLimiter) Acquire(ctx context.Context, _ domain.Channel) (time.Time, error) {
	if !l.granted {
		l.granted = true
		return time.Now(), nil
	}
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}

// zeroSlotLimiter returns success without a slot, like the deadline race the Redis limiter had.
type zeroSlotLimiter struct{}

func (zeroSlotLimiter) Acquire(context.Context, domain.Channel) (time.Time, error) {
	return time.Time{}, nil
}

func TestDeliverRefusesAPermitWithoutASlot(t *testing.T) {
	f := newDeliverFixture()
	f.uc.d.Limiter = zeroSlotLimiter{}
	msg := f.queue("n1", nil)
	if d := f.uc.Handle(context.Background(), msg); d != ports.DecisionRequeue {
		t.Fatalf("decision %s, want requeue", d)
	}
	if f.provider.callCount() != 0 || f.repo.get("n1").Status != domain.StatusQueued {
		t.Fatal("a permit without a slot must fail closed: no claim, no send")
	}
}
