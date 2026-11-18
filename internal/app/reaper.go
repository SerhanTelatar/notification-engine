package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// DefaultReapLimit bounds the rows one reaper pass recovers.
const DefaultReapLimit = 500

// ReaperDeps are the Reaper's collaborators and thresholds.
type ReaperDeps struct {
	Repo           ports.NotificationRepository
	Publisher      ports.Publisher
	Events         ports.StatusBroadcaster
	Metrics        ports.MetricsRecorder
	Clock          ports.Clock
	Log            *slog.Logger
	SendingTimeout time.Duration // REAPER_SENDING_TIMEOUT
	QueuedTimeout  time.Duration // REAPER_QUEUED_TIMEOUT
	Limit          int           // 0 = DefaultReapLimit
}

// Reaper recovers notifications stranded by a crash (research R5), staying inside the FR-012
// state machine:
//   - sending for longer than SendingTimeout: the worker died around the provider call. Record an
//     abandoned attempt (the call may have reached the provider) and go to retrying, or failed if
//     that was the last attempt. This is the documented at-least-once window (ADR 0003).
//   - queued for longer than QueuedTimeout: the message was lost. Republish it without a status
//     change; a duplicate is dropped by the consumer guard.
type Reaper struct{ d ReaperDeps }

// NewReaper returns the crash-recovery use case.
func NewReaper(d ReaperDeps) *Reaper {
	if d.Limit <= 0 {
		d.Limit = DefaultReapLimit
	}
	return &Reaper{d: d}
}

// Tick runs one recovery pass.
func (r *Reaper) Tick(ctx context.Context) error {
	return errors.Join(r.reapSending(ctx), r.republishQueued(ctx))
}

func (r *Reaper) reapSending(ctx context.Context) error {
	d := r.d
	now := d.Clock.Now()
	decide := func(n domain.Notification) ports.DeliveryOutcome {
		attemptNo := n.Attempts + 1
		msg := fmt.Sprintf("worker stopped during the provider call; recovered after %s in sending", now.Sub(n.UpdatedAt).Round(time.Second))
		o := ports.DeliveryOutcome{
			NotificationID: n.ID, At: now, LastError: &msg,
			Attempt: domain.Attempt{NotificationID: n.ID, AttemptNo: attemptNo, StartedAt: n.UpdatedAt,
				FinishedAt: now, Outcome: domain.OutcomeAbandoned, Error: &msg},
		}
		if domain.ShouldFail(attemptNo, n.MaxAttempts) {
			o.To = domain.StatusFailed
		} else {
			o.To, o.NextAttemptAt = domain.StatusRetrying, now
		}
		return o
	}
	reaped, err := d.Repo.ReapStaleSending(ctx, now.Add(-d.SendingTimeout), d.Limit, decide)
	if err != nil {
		return fmt.Errorf("reap stale sending: %w", err)
	}
	if len(reaped) == 0 {
		return nil
	}
	d.Metrics.Reaped("sending", len(reaped))
	for i := range reaped {
		n := &reaped[i]
		if n.Status == domain.StatusFailed {
			d.Metrics.DeadLettered(n.Channel, ports.ReasonExhausted)
		}
		d.Log.WarnContext(ctx, "recovered a notification stuck in sending", attrNotificationID, n.ID,
			attrCorrelationID, n.CorrelationID, "to", n.Status, "attempts", n.Attempts)
		broadcast(ctx, d.Log, d.Events, n, statusPtr(domain.StatusSending), now)
	}
	return nil
}

func (r *Reaper) republishQueued(ctx context.Context) error {
	d := r.d
	now := d.Clock.Now()
	publish := func(ctx context.Context, rows []domain.Notification) error {
		msgs := make([]ports.Message, len(rows))
		for i, n := range rows {
			msgs[i] = ports.Message{NotificationID: n.ID, Channel: n.Channel, Priority: n.Priority,
				CorrelationID: n.CorrelationID, TraceParent: n.TraceParent, EnqueuedAt: now}
			d.Log.WarnContext(ctx, "republishing a notification stuck in queued", attrNotificationID, n.ID,
				attrCorrelationID, n.CorrelationID, "queued_for", now.Sub(n.UpdatedAt).Round(time.Second).String())
		}
		return d.Publisher.Publish(ctx, msgs) //nolint:wrapcheck // wrapped by the repository
	}
	n, err := d.Repo.RepublishStaleQueued(ctx, now.Add(-d.QueuedTimeout), now, d.Limit, publish)
	if err != nil {
		return fmt.Errorf("republish stale queued: %w", err)
	}
	if n > 0 {
		d.Metrics.Reaped("queued", n)
	}
	return nil
}
