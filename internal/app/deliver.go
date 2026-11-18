package app

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Default bounds for the delivery pipeline.
const (
	// DefaultWorkTimeout bounds the claimed work (claim, provider call, outcome commit). It's
	// detached from shutdown so in-flight sends finish (research R17); the provider call itself
	// is bounded by PROVIDER_TIMEOUT inside the adapter.
	DefaultWorkTimeout = 30 * time.Second
	limiterRetryMin    = 25 * time.Millisecond
	limiterRetryMax    = time.Second
)

// DelivererDeps are the delivery pipeline's collaborators and policy.
type DelivererDeps struct {
	Repo     ports.NotificationRepository
	Provider ports.Provider
	Breaker  ports.CircuitBreaker
	Limiter  ports.RateLimiter
	Events   ports.StatusBroadcaster
	Metrics  ports.MetricsRecorder
	Clock    ports.Clock
	Log      *slog.Logger

	BackoffBase    time.Duration // BACKOFF_BASE
	BackoffMax     time.Duration // BACKOFF_MAX
	LimiterMaxWait time.Duration // LIMITER_MAX_WAIT
	// SendOffset (RATE_LIMIT_SEND_OFFSET) delays every provider call to its reserved slot plus this
	// constant, so variable claim latency can't bunch calls together; it should cover a normal
	// claim. 0 sends as soon as the claim commits.
	SendOffset time.Duration
	// MaxDeferAge (BREAKER_MAX_DEFER_AGE) caps breaker deferrals, which use no attempts: once a
	// notification has waited this long since max(created_at, scheduled_at), it fails as
	// provider_unavailable instead of deferring again (analysis finding D1, Constitution III).
	// 0 = no cap.
	MaxDeferAge time.Duration
	WorkTimeout time.Duration // 0 = DefaultWorkTimeout
	Rand        func(n int64) int64
}

// Deliverer is the delivery pipeline for one work message (research R6):
//
//	breaker check → rate-limit slot → guarded queued→sending → wait for slot + send offset
//	→ provider call via the breaker → attempt row + sent | retrying | failed in one transaction
//	→ ack / requeue / dead-letter.
type Deliverer struct{ d DelivererDeps }

// NewDeliverer returns the delivery use case.
func NewDeliverer(d DelivererDeps) *Deliverer {
	if d.WorkTimeout <= 0 {
		d.WorkTimeout = DefaultWorkTimeout
	}
	if d.Rand == nil {
		d.Rand = rand.Int64N
	}
	return &Deliverer{d: d}
}

// Handle processes one message. It's a ports.MessageHandler.
func (dl *Deliverer) Handle(ctx context.Context, msg ports.Message) ports.Decision {
	d := dl.d
	log := d.Log.With(attrNotificationID, msg.NotificationID, attrCorrelationID, msg.CorrelationID, "channel", msg.Channel)

	// 1. Breaker open: defer without spending a permit or an attempt (FR-024).
	if err := d.Breaker.Allow(msg.Channel); err != nil {
		return dl.deferOpen(ctx, log, msg, err)
	}

	// 2. Rate-limit permit, before the claim, so the notification stays cancellable while it's
	//    throttled (research R6). The wait is capped (Constitution III: every retry has a cap).
	slot, decision, ok := dl.acquire(ctx, log, msg.Channel)
	if !ok {
		return decision
	}

	// From here on the work is claimed: finish it even if shutdown starts (research R17).
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.WorkTimeout)
	defer cancel()

	// 3. Guarded claim: the atomic cancel check (FR-014) and duplicate guard (FR-026).
	n, ok, err := d.Repo.StartSending(work, msg.NotificationID, d.Clock.Now())
	if err != nil {
		log.ErrorContext(ctx, "claim failed, message requeued", "error", err)
		return ports.DecisionRequeue
	}
	if !ok {
		log.InfoContext(ctx, "notification not queued any more, message dropped")
		return ports.DecisionAck
	}
	broadcast(work, log, d.Events, n, statusPtr(domain.StatusQueued), n.UpdatedAt)

	// 4. Pace the call on the reserved slot (SC-003 at the provider, not only at the permit).
	if !dl.pace(work, log, n, slot) {
		return ports.DecisionAck // released back to retrying, already logged
	}

	// 5. Provider call through the channel's breaker.
	started := d.Clock.Now()
	res, err := d.Breaker.Call(work, n.Channel, func(c context.Context) ports.ProviderResult {
		return d.Provider.Send(c, ports.ProviderRequest{NotificationID: n.ID, CorrelationID: n.CorrelationID,
			To: n.Recipient, Channel: n.Channel, Content: n.Content})
	})
	if err != nil {
		// The breaker opened between Allow and Call: same deferral, already claimed.
		return dl.deferClaimed(work, log, n, err)
	}
	finished := d.Clock.Now()
	d.Metrics.Delivered(n.Channel, res.Outcome, res.Latency)

	// 6. Record the attempt and the outcome atomically.
	return dl.record(work, log, n, res, started, finished)
}

// pace holds the provider call until slot + SendOffset. Permits are spaced exactly 1/limit apart,
// but the claim between permit and call takes a variable time; without pacing, calls bunch and a
// sliding second at the provider can exceed the limit. If the claim overran the offset, the slot
// is stale: reserve a fresh one rather than sending early. If no fresh slot comes within the
// maximum wait, move the claimed row back to retrying without using an attempt (Constitution III:
// capped, and it ends somewhere legal). ok=false means the row was released.
func (dl *Deliverer) pace(ctx context.Context, log *slog.Logger, n *domain.Notification, slot time.Time) bool {
	d := dl.d
	if d.SendOffset <= 0 {
		return true
	}
	target := slot.Add(d.SendOffset)
	if time.Now().After(target) {
		fresh, _, ok := dl.acquire(ctx, log, n.Channel)
		if !ok {
			next := d.Clock.Now()
			if err := d.Repo.DeferSending(ctx, ports.Deferral{NotificationID: n.ID, NextAttemptAt: next, At: next}); err != nil {
				log.ErrorContext(ctx, "releasing an unpaced claim failed; the reaper will recover it", "error", err)
			}
			return false
		}
		target = fresh.Add(d.SendOffset)
	}
	if wait := time.Until(target); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}
	return true
}

// acquire waits for a slot for at most LimiterMaxWait, retrying limiter errors with backoff and
// failing closed. ok=false means: release the message with the returned decision.
func (dl *Deliverer) acquire(ctx context.Context, log *slog.Logger, ch domain.Channel) (time.Time, ports.Decision, bool) {
	d := dl.d
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, d.LimiterMaxWait)
	defer cancel()
	pause := limiterRetryMin
	for {
		slot, err := d.Limiter.Acquire(wctx, ch)
		if err == nil && slot.IsZero() {
			// Defence in depth: a permit always carries its slot. Treat anything else as no permit,
			// so a limiter bug fails closed rather than open.
			err = errors.New("rate limiter returned no slot")
		}
		if err == nil {
			d.Metrics.LimiterWait(ch, time.Since(start))
			return slot, ports.DecisionAck, true
		}
		if ctx.Err() != nil { // shutdown: hand the message back right away
			return time.Time{}, ports.DecisionRequeue, false
		}
		if wctx.Err() != nil {
			d.Metrics.LimiterTimeout(ch)
			log.WarnContext(ctx, "no rate-limit permit within the maximum wait, message requeued",
				"max_wait", d.LimiterMaxWait.String(), "error", err)
			return time.Time{}, ports.DecisionRequeue, false
		}
		log.WarnContext(ctx, "rate limiter unavailable, retrying (fail closed)", "error", err)
		select {
		case <-time.After(pause):
		case <-wctx.Done():
		}
		pause = min(pause*2, limiterRetryMax)
	}
}

// record applies the provider result: sent, retrying with backoff, or failed and dead-lettered.
func (dl *Deliverer) record(ctx context.Context, log *slog.Logger, n *domain.Notification, res ports.ProviderResult, started, finished time.Time) ports.Decision {
	d := dl.d
	attemptNo := n.Attempts + 1
	o := ports.DeliveryOutcome{
		NotificationID: n.ID,
		At:             finished,
		Attempt: domain.Attempt{
			NotificationID: n.ID, AttemptNo: attemptNo, StartedAt: started, FinishedAt: finished,
			LatencyMS: int(res.Latency.Milliseconds()), HTTPStatus: res.HTTPStatus, Outcome: res.Outcome,
		},
	}
	if res.Error != "" {
		msg := res.Error
		o.Attempt.Error, o.LastError = &msg, &msg
	}

	decision, reason := ports.DecisionAck, ""
	switch {
	case res.Outcome == domain.OutcomeSuccess:
		o.To, o.ProviderMessageID = domain.StatusSent, res.MessageID
		if res.MessageID == nil {
			log.WarnContext(ctx, "provider accepted without a parsable message id")
		}
	case res.Outcome == domain.OutcomeTransient && !domain.ShouldFail(attemptNo, n.MaxAttempts):
		o.To = domain.StatusRetrying
		o.NextAttemptAt = finished.Add(domain.Backoff(attemptNo, d.BackoffBase, d.BackoffMax, res.RetryAfter, d.Rand))
	case res.Outcome == domain.OutcomeTransient:
		o.To, decision, reason = domain.StatusFailed, ports.DecisionDeadLetter, ports.ReasonExhausted
	default:
		o.To, decision, reason = domain.StatusFailed, ports.DecisionDeadLetter, ports.ReasonPermanent
	}

	if err := d.Repo.ApplyOutcome(ctx, o); err != nil {
		// The row stays sending and the reaper recovers it (research R5). Redelivering the
		// message wouldn't help: the guard would drop it.
		log.ErrorContext(ctx, "recording the delivery outcome failed; the reaper will recover it",
			"outcome", res.Outcome, "error", err)
		return ports.DecisionAck
	}

	n.Status, n.Attempts, n.UpdatedAt, n.LastError = o.To, attemptNo, finished, o.LastError
	switch o.To {
	case domain.StatusSent:
		d.Metrics.EndToEnd(n.Channel, n.Priority, finished.Sub(n.DeliveryReference()))
		log.InfoContext(ctx, "notification sent", "attempt", attemptNo)
	case domain.StatusRetrying:
		d.Metrics.Retried(n.Channel)
		log.WarnContext(ctx, "delivery failed, will retry", "attempt", attemptNo,
			"next_attempt_at", o.NextAttemptAt, "error", res.Error)
	case domain.StatusFailed:
		d.Metrics.DeadLettered(n.Channel, reason)
		log.ErrorContext(ctx, "notification failed", "attempt", attemptNo, "reason", reason, "error", res.Error)
	}
	broadcast(ctx, log, d.Events, n, statusPtr(domain.StatusSending), finished)
	return decision
}

// deferOpen handles a breaker that's open before the claim: claim, then move back to retrying
// without an attempt (FR-024). The maximum deferral age is added with US7.
func (dl *Deliverer) deferOpen(ctx context.Context, log *slog.Logger, msg ports.Message, openErr error) ports.Decision {
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), dl.d.WorkTimeout)
	defer cancel()
	n, ok, err := dl.d.Repo.StartSending(work, msg.NotificationID, dl.d.Clock.Now())
	if err != nil {
		log.ErrorContext(ctx, "claim failed, message requeued", "error", err)
		return ports.DecisionRequeue
	}
	if !ok {
		return ports.DecisionAck
	}
	return dl.deferClaimed(work, log, n, openErr)
}

func (dl *Deliverer) deferClaimed(ctx context.Context, log *slog.Logger, n *domain.Notification, openErr error) ports.Decision {
	d := dl.d
	now := d.Clock.Now()
	retryIn := time.Second
	var boe *ports.BreakerOpenError
	if errors.As(openErr, &boe) && boe.RetryIn > 0 {
		retryIn = boe.RetryIn
	}
	if d.MaxDeferAge > 0 && now.Sub(n.DeliveryReference()) > d.MaxDeferAge {
		return dl.failUnavailable(ctx, log, n, now)
	}
	next := now.Add(retryIn + time.Duration(d.Rand(int64(retryIn/4)+1))) // jitter spreads the trial wave
	if d.MaxDeferAge > 0 {
		// Come back no later than the cap, so it's enforced on time rather than up to a whole
		// cool-down late.
		if capAt := n.DeliveryReference().Add(d.MaxDeferAge + time.Millisecond); next.After(capAt) {
			next = capAt
		}
	}
	if err := d.Repo.DeferSending(ctx, ports.Deferral{NotificationID: n.ID, NextAttemptAt: next, At: now}); err != nil {
		log.ErrorContext(ctx, "deferral failed; the reaper will recover the row", "error", err)
		return ports.DecisionAck
	}
	d.Metrics.Delivered(n.Channel, domain.OutcomeDeferred, 0)
	n.Status, n.NextAttemptAt, n.UpdatedAt = domain.StatusRetrying, next, now
	log.InfoContext(ctx, "circuit open, delivery deferred without using an attempt", "next_attempt_at", next)
	broadcast(ctx, log, d.Events, n, statusPtr(domain.StatusSending), now)
	return ports.DecisionAck
}

// failUnavailable ends a deferral that has waited longer than MaxDeferAge: sending → failed with
// last_error=provider_unavailable, no attempt consumed, and the message is dead-lettered.
func (dl *Deliverer) failUnavailable(ctx context.Context, log *slog.Logger, n *domain.Notification, now time.Time) ports.Decision {
	d := dl.d
	reason := ports.ReasonProviderUnavailable
	if err := d.Repo.DeferSending(ctx, ports.Deferral{NotificationID: n.ID, Fail: true, LastError: &reason, At: now}); err != nil {
		log.ErrorContext(ctx, "failing an over-age deferral failed; the reaper will recover the row", "error", err)
		return ports.DecisionAck
	}
	d.Metrics.DeadLettered(n.Channel, reason)
	failedAt := now
	n.Status, n.LastError, n.UpdatedAt, n.FailedAt = domain.StatusFailed, &reason, now, &failedAt
	log.ErrorContext(ctx, "provider unavailable for longer than the maximum deferral age, notification failed",
		"waited", now.Sub(n.DeliveryReference()).Round(time.Second).String(), "max_defer_age", d.MaxDeferAge.String())
	broadcast(ctx, log, d.Events, n, statusPtr(domain.StatusSending), now)
	return ports.DecisionDeadLetter
}
