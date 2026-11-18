package app

import (
	"bytes"
	"context"
	"errors"
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
	TraceParent    string // W3C trace context to carry through the outbox (tracing on)
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
	Templates   ports.TemplateRepository // nil: every template_id is unknown
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
	tpl, err := newTemplateCache(c.d.Templates).resolve(ctx, req)
	if err != nil {
		return CreateResult{}, err
	}
	n, err := buildNotification(req, tpl, c.d.IDs.NewID(), c.d.Clock.Now(), c.d.MaxAttempts, nil)
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

// renderPlaceholder stands in for content that couldn't be rendered, so the rest of the request
// is still validated and every problem is reported at once (FR-005). It fits every channel.
const renderPlaceholder = "-"

// buildNotification validates one request and builds its aggregate. Single creates and batch
// items share it, so they validate identically. tpl is the resolved template for a template
// request (nil when unknown). The error is a *domain.ValidationError.
func buildNotification(req CreateRequest, tpl *domain.Template, id string, now time.Time, maxAttempts int, batchID *string) (*domain.Notification, error) {
	if verr := domain.CheckContentSource(req.Content, req.TemplateID); verr != nil {
		return nil, verr
	}
	verr := &domain.ValidationError{}
	var content string
	switch {
	case req.Content != nil:
		content = *req.Content
	case tpl == nil:
		verr.Add("template_id", "unknown template")
		content = renderPlaceholder
	default:
		// Rendered at submission (research R12): what's stored and sent is this snapshot, and
		// the channel limits below apply to it.
		rendered, err := tpl.Render(req.Variables)
		var rerr *domain.ValidationError
		switch {
		case errors.As(err, &rerr):
			verr.Merge("", rerr)
			content = renderPlaceholder
		case err != nil:
			return nil, fmt.Errorf("render template %s: %w", tpl.ID, err)
		default:
			content = rendered
		}
	}
	draft := domain.Draft{
		Recipient: req.Recipient, Channel: req.Channel, Content: content, Priority: req.Priority,
		ScheduledAt: req.ScheduledAt, IdempotencyKey: req.IdempotencyKey, CorrelationID: req.CorrelationID,
		TraceParent: req.TraceParent, BatchID: batchID,
	}
	if req.Content == nil { // a template request keeps its id and variables for audit and the hash
		draft.TemplateID, draft.Variables = req.TemplateID, req.Variables
	}
	n, err := domain.NewNotification(draft, id, now, maxAttempts)
	var nerr *domain.ValidationError
	switch {
	case errors.As(err, &nerr):
		verr.Merge("", nerr)
	case err != nil:
		return nil, err //nolint:wrapcheck // configuration error from the domain
	}
	if err := verr.OrNil(); err != nil {
		return nil, err
	}
	return n, nil
}
