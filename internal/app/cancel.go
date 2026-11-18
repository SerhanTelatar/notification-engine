package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// CancellerDeps are the Canceller's collaborators.
type CancellerDeps struct {
	Repo    ports.NotificationRepository
	Batches ports.BatchRepository
	Events  ports.StatusBroadcaster
	Clock   ports.Clock
	Log     *slog.Logger
}

// Canceller implements notification and batch cancellation (US5). A cancelled notification is
// never delivered: the consumer's guarded queued → sending claim fails for it (FR-014).
type Canceller struct{ d CancellerDeps }

// NewCanceller returns the cancel use cases.
func NewCanceller(d CancellerDeps) *Canceller { return &Canceller{d: d} }

// Cancel cancels one notification (FR-013). Re-cancelling succeeds without change; sending, sent
// or failed is a *domain.NotCancellableError carrying the current status; an unknown id is
// domain.ErrNotFound.
func (c *Canceller) Cancel(ctx context.Context, id string) (*domain.Notification, error) {
	now := c.d.Clock.Now()
	o, err := c.d.Repo.Cancel(ctx, id, now)
	if err != nil {
		return nil, fmt.Errorf("cancel %s: %w", id, err)
	}
	n := o.Notification
	if o.Changed {
		c.d.Log.InfoContext(ctx, "notification cancelled", attrNotificationID, n.ID, "from", o.Previous)
		prev := o.Previous
		broadcast(ctx, c.d.Log, c.d.Events, n, &prev, now)
		return n, nil
	}
	if n.Status == domain.StatusCancelled {
		return n, nil // idempotent
	}
	return nil, &domain.NotCancellableError{Current: n.Status}
}

// CancelBatch cancels every cancellable notification of a batch and reports how many were
// cancelled, already cancelled, or past cancelling (FR-015).
func (c *Canceller) CancelBatch(ctx context.Context, id string) (ports.BatchCancelResult, error) {
	now := c.d.Clock.Now()
	res, err := c.d.Batches.CancelBatch(ctx, id, now)
	if err != nil {
		return ports.BatchCancelResult{}, fmt.Errorf("cancel batch %s: %w", id, err)
	}
	for _, o := range res.Rows {
		prev := o.Previous
		broadcast(ctx, c.d.Log, c.d.Events, o.Notification, &prev, now)
	}
	c.d.Log.InfoContext(ctx, "batch cancelled", "batch_id", id, "cancelled", res.Cancelled,
		"already_cancelled", res.AlreadyCancelled, "not_cancellable", res.NotCancellable)
	return res, nil
}
