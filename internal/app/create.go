package app

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// CreateRequest is a single notification as submitted (FR-001). Content and TemplateID are
// mutually exclusive (FR-004).
type CreateRequest struct {
	Recipient      string
	Channel        string
	Content        *string
	Priority       string
	ScheduledAt    *time.Time
	IdempotencyKey *string
	TemplateID     *string
	Variables      map[string]string
	CorrelationID  string
}

// CreateResult is the stored notification. Replayed is true when an idempotency key matched an
// earlier identical request and the original was returned (FR-006).
type CreateResult struct {
	Notification *domain.Notification
	Replayed     bool
}

// CreatorDeps are the Creator's collaborators.
type CreatorDeps struct {
	Repo        ports.NotificationRepository
	IDs         ports.IDGenerator
	Clock       ports.Clock
	Events      ports.StatusBroadcaster
	Log         *slog.Logger
	MaxAttempts int
}

// Creator implements CreateNotification: validate, persist (only the DB, no dual writes,
// Constitution III), resolve idempotency, and announce the new status after commit.
type Creator struct{ d CreatorDeps }

// NewCreator returns the create use case.
func NewCreator(d CreatorDeps) *Creator { return &Creator{d: d} }

// Create validates and stores one notification. Delivery happens asynchronously.
func (c *Creator) Create(ctx context.Context, req CreateRequest) (CreateResult, error) {
	n, err := buildNotification(req, c.d.IDs.NewID(), c.d.Clock.Now(), c.d.MaxAttempts, nil)
	if err != nil {
		return CreateResult{}, err
	}

	stored, created, err := c.d.Repo.Create(ctx, n)
	if err != nil {
		return CreateResult{}, fmt.Errorf("store notification: %w", err)
	}
	if !created {
		// The idempotency key already exists: same payload replays, different payload conflicts.
		if !bytes.Equal(stored.PayloadHash, n.PayloadHash) {
			return CreateResult{}, fmt.Errorf("key %q: %w", *n.IdempotencyKey, domain.ErrIdempotencyConflict)
		}
		c.d.Log.InfoContext(ctx, "idempotent replay", attrNotificationID, stored.ID)
		return CreateResult{Notification: stored, Replayed: true}, nil
	}

	c.d.Log.InfoContext(ctx, "notification accepted", attrNotificationID, stored.ID,
		"channel", stored.Channel, "priority", stored.Priority.String(), "status", stored.Status)
	broadcast(ctx, c.d.Log, c.d.Events, stored, nil, stored.CreatedAt)
	return CreateResult{Notification: stored}, nil
}

// buildNotification validates one request and builds its aggregate. Single creates and batch
// items share it, so they validate identically. The error is a *domain.ValidationError.
func buildNotification(req CreateRequest, id string, now time.Time, maxAttempts int, batchID *string) (*domain.Notification, error) {
	if verr := domain.CheckContentSource(req.Content, req.TemplateID); verr != nil {
		return nil, verr
	}
	if req.TemplateID != nil {
		// Templates arrive with US8; until then every template id is unknown (spec US8-AS4).
		return nil, domain.NewFieldError("template_id", "unknown template")
	}
	draft := domain.Draft{
		Recipient: req.Recipient, Channel: req.Channel, Content: *req.Content, Priority: req.Priority,
		ScheduledAt: req.ScheduledAt, IdempotencyKey: req.IdempotencyKey, CorrelationID: req.CorrelationID,
		BatchID: batchID,
	}
	return domain.NewNotification(draft, id, now, maxAttempts) //nolint:wrapcheck // domain validation error
}
