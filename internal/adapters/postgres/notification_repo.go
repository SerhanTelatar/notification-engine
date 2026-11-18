package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres/sqlcgen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// errNotYet marks repository methods whose user story hasn't landed (US2, US5, US6, US7).
var errNotYet = errors.New("not implemented yet")

// NotificationRepo implements ports.NotificationRepository on PostgreSQL.
type NotificationRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

var _ ports.NotificationRepository = (*NotificationRepo)(nil)

// NewNotificationRepo returns a repository on pool.
func NewNotificationRepo(pool *pgxpool.Pool) *NotificationRepo {
	return &NotificationRepo{pool: pool, q: sqlcgen.New(pool)}
}

// Create inserts n. An existing idempotency key returns the stored row with created=false
// (data-model.md notifications.idempotency_key); the caller compares notifications.payload_hash.
func (r *NotificationRepo) Create(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	params, err := insertParams(n)
	if err != nil {
		return nil, false, err
	}
	row, err := r.q.InsertNotification(ctx, params)
	if err == nil {
		out, err := toDomain(row)
		return out, true, err
	}
	if !errors.Is(err, pgx.ErrNoRows) || n.IdempotencyKey == nil {
		return nil, false, translate(err, "insert notification")
	}
	// ON CONFLICT DO NOTHING: the key exists. The conflicting insert waited for the other
	// transaction to commit, so the original row is visible now.
	existing, err := r.q.GetByIdempotencyKey(ctx, n.IdempotencyKey)
	if err != nil {
		return nil, false, translate(err, "read notification by idempotency key")
	}
	out, err := toDomain(existing)
	return out, false, err
}

// RelayTx runs one channel's relay pass in a single transaction (research R4).
func (r *NotificationRepo) RelayTx(ctx context.Context, ch domain.Channel, at time.Time, quota ports.QuotaFunc, publish ports.PublishFunc) (ports.RelayResult, error) {
	var res ports.RelayResult
	err := WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		locked, err := q.RelayTryLock(ctx, string(ch))
		if err != nil {
			return translate(err, "relay lock")
		}
		if !locked {
			res.Skipped = "locked"
			return nil
		}
		queued, err := q.CountQueued(ctx, string(ch))
		if err != nil {
			return translate(err, "count queued")
		}
		limit := quota(int(queued))
		if limit <= 0 {
			res.Skipped = "backpressure"
			return nil
		}
		rows, err := q.PickDue(ctx, sqlcgen.PickDueParams{Channel: string(ch), Now: at, PickLimit: int32(min(limit, 1<<30))}) //nolint:gosec // bounded above
		if err != nil {
			return translate(err, "pick due")
		}
		if len(rows) == 0 {
			return nil
		}
		ids := make([]uuid.UUID, len(rows))
		for i, row := range rows {
			ids[i] = row.ID
		}
		marked, err := q.MarkQueued(ctx, sqlcgen.MarkQueuedParams{Ids: ids, Now: at})
		if err != nil {
			return translate(err, "mark queued")
		}
		if len(marked) != len(ids) {
			// Rows are locked FOR UPDATE, so this can only mean a broken invariant.
			return fmt.Errorf("mark queued: %d of %d rows: %w", len(marked), len(ids), domain.ErrInvalidTransition)
		}
		picked := make([]domain.Notification, len(rows))
		for i, row := range rows {
			n, err := toDomain(row)
			if err != nil {
				return err
			}
			// Rows keep their pre-relay status (pending/scheduled/retrying) so the caller can
			// report the transition; in the database they're queued now.
			n.UpdatedAt = at
			picked[i] = *n
		}
		// Publish inside the transaction; a failure rolls the rows back to due (ADR 0002).
		if err := publish(ctx, picked); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		res.Published = len(picked)
		return nil
	})
	if err != nil {
		return ports.RelayResult{}, err
	}
	return res, nil
}

// StartSending is the guarded queued → sending claim (FR-014). ok=false means the row isn't
// queued: cancelled, duplicate message or already processed. It locks the row first, so a message
// that overtakes its relay transaction waits for the commit instead of being dropped.
func (r *NotificationRepo) StartSending(ctx context.Context, id string, at time.Time) (*domain.Notification, bool, error) {
	uid, err := parseID(id)
	if err != nil {
		return nil, false, err
	}
	var claimed *domain.Notification
	err = WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		// Wait for any relay transaction holding the row, then see its committed status.
		if _, err := q.LockForClaim(ctx, uid); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // unknown id: nothing to claim
			}
			return translate(err, "lock for claim")
		}
		row, err := q.StartSending(ctx, sqlcgen.StartSendingParams{ID: uid, Now: at})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // not queued: cancelled, duplicate or already processed
		}
		if err != nil {
			return translate(err, "start sending")
		}
		claimed, err = toDomain(row)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return claimed, claimed != nil, nil
}

// ApplyOutcome records the attempt and applies the transition in one transaction.
func (r *NotificationRepo) ApplyOutcome(ctx context.Context, o ports.DeliveryOutcome) error {
	return WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		return applyOutcome(ctx, r.q.WithTx(tx), o)
	})
}

func applyOutcome(ctx context.Context, q *sqlcgen.Queries, o ports.DeliveryOutcome) error {
	id, err := parseID(o.NotificationID)
	if err != nil {
		return err
	}
	lastErr := truncated(o.LastError) // data-model.md notifications.last_error (1 KB)
	var attempts int32
	switch o.To {
	case domain.StatusSent:
		attempts, err = q.MarkSent(ctx, sqlcgen.MarkSentParams{ID: id, ProviderMessageID: o.ProviderMessageID, Now: o.At})
	case domain.StatusRetrying:
		attempts, err = q.MarkRetrying(ctx, sqlcgen.MarkRetryingParams{ID: id, NextAttemptAt: o.NextAttemptAt, LastError: lastErr, Now: o.At})
	case domain.StatusFailed:
		attempts, err = q.MarkFailed(ctx, sqlcgen.MarkFailedParams{ID: id, LastError: lastErr, Now: o.At})
	default:
		return fmt.Errorf("outcome status %q: %w", o.To, domain.ErrInvalidTransition)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("sending → %s for %s: %w", o.To, o.NotificationID, domain.ErrInvalidTransition)
	}
	if err != nil {
		return translate(err, "apply outcome")
	}

	a := o.Attempt
	var httpStatus *int32
	if a.HTTPStatus != nil {
		s := int32(*a.HTTPStatus) //nolint:gosec // HTTP status codes fit in int32
		httpStatus = &s
	}
	err = q.InsertAttempt(ctx, sqlcgen.InsertAttemptParams{
		NotificationID: id,
		AttemptNo:      attempts, // the attempt counter after the increment
		StartedAt:      a.StartedAt,
		FinishedAt:     a.FinishedAt,
		LatencyMs:      int32(min(max(a.LatencyMS, 0), 1<<30)), //nolint:gosec // clamped
		HttpStatus:     httpStatus,
		Outcome:        string(a.Outcome),
		Error:          truncated(a.Error), // data-model.md notification_attempts.error (1 KB)
	})
	return translate(err, "insert attempt")
}

// DeferSending moves a sending row back to retrying without consuming an attempt (breaker open),
// or to failed with provider_unavailable once the maximum deferral age is exceeded (FR-024).
func (r *NotificationRepo) DeferSending(ctx context.Context, d ports.Deferral) error {
	id, err := parseID(d.NotificationID)
	if err != nil {
		return err
	}
	if d.Fail {
		_, err = r.q.DeferFail(ctx, sqlcgen.DeferFailParams{ID: id, LastError: truncated(d.LastError), Now: d.At})
	} else {
		_, err = r.q.DeferSending(ctx, sqlcgen.DeferSendingParams{ID: id, NextAttemptAt: d.NextAttemptAt, Now: d.At})
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("defer %s: %w", d.NotificationID, domain.ErrInvalidTransition)
	}
	return translate(err, "defer sending")
}

// ReapStaleSending locks stale sending rows and applies decide to each, in one transaction.
func (r *NotificationRepo) ReapStaleSending(ctx context.Context, olderThan time.Time, limit int, decide ports.ReapDecision) ([]domain.Notification, error) {
	var out []domain.Notification
	err := WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		rows, err := q.ReapStaleSending(ctx, sqlcgen.ReapStaleSendingParams{OlderThan: olderThan, PickLimit: int32(min(limit, 1<<30))}) //nolint:gosec // bounded
		if err != nil {
			return translate(err, "pick stale sending")
		}
		for _, row := range rows {
			n, err := toDomain(row)
			if err != nil {
				return err
			}
			o := decide(*n)
			if err := applyOutcome(ctx, q, o); err != nil {
				return err
			}
			n.Status, n.Attempts, n.UpdatedAt, n.LastError = o.To, n.Attempts+1, o.At, o.LastError
			out = append(out, *n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RepublishStaleQueued republishes rows stuck in queued without changing their status (R5).
func (r *NotificationRepo) RepublishStaleQueued(ctx context.Context, olderThan, at time.Time, limit int, publish ports.PublishFunc) (int, error) {
	n := 0
	err := WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		rows, err := q.PickStaleQueued(ctx, sqlcgen.PickStaleQueuedParams{OlderThan: olderThan, PickLimit: int32(min(limit, 1<<30))}) //nolint:gosec // bounded
		if err != nil {
			return translate(err, "pick stale queued")
		}
		if len(rows) == 0 {
			return nil
		}
		stale := make([]domain.Notification, len(rows))
		ids := make([]uuid.UUID, len(rows))
		for i, row := range rows {
			d, err := toDomain(row)
			if err != nil {
				return err
			}
			stale[i], ids[i] = *d, row.ID
		}
		if err := publish(ctx, stale); err != nil {
			return fmt.Errorf("republish: %w", err)
		}
		if err := q.TouchQueued(ctx, sqlcgen.TouchQueuedParams{Ids: ids, Now: at}); err != nil {
			return translate(err, "touch queued")
		}
		n = len(rows)
		return nil
	})
	return n, err
}

// Get returns one notification; an unknown or malformed id is domain.ErrNotFound.
func (r *NotificationRepo) Get(ctx context.Context, id string) (*domain.Notification, error) {
	uid, err := parseID(id)
	if err != nil {
		return nil, err
	}
	row, err := r.q.GetNotification(ctx, uid)
	if err != nil {
		return nil, translate(err, "get notification")
	}
	return toDomain(row)
}

// ListAttempts returns a notification's attempt history ordered by attempt_no (FR-009).
func (r *NotificationRepo) ListAttempts(ctx context.Context, id string) ([]domain.Attempt, error) {
	uid, err := parseID(id)
	if err != nil {
		return nil, err
	}
	rows, err := r.q.ListAttempts(ctx, uid)
	if err != nil {
		return nil, translate(err, "list attempts")
	}
	out := make([]domain.Attempt, len(rows))
	for i, a := range rows {
		out[i] = domain.Attempt{
			NotificationID: a.NotificationID.String(),
			AttemptNo:      int(a.AttemptNo),
			StartedAt:      a.StartedAt.UTC(),
			FinishedAt:     a.FinishedAt.UTC(),
			LatencyMS:      int(a.LatencyMs),
			Outcome:        domain.Outcome(a.Outcome),
			Error:          a.Error,
		}
		if a.HttpStatus != nil {
			s := int(*a.HttpStatus)
			out[i].HTTPStatus = &s
		}
	}
	return out, nil
}

// Cancel cancels one notification if its status allows it (FR-013). The row is locked first, so
// a cancel that races an open relay transaction waits for its commit and decides on the committed
// status (the same reason as StartSending).
func (r *NotificationRepo) Cancel(ctx context.Context, id string, at time.Time) (ports.CancelOutcome, error) {
	uid, err := parseID(id)
	if err != nil {
		return ports.CancelOutcome{}, err
	}
	var out ports.CancelOutcome
	err = WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		prev, err := q.LockForClaim(ctx, uid)
		if err != nil {
			return translate(err, "lock for cancel")
		}
		previous, err := domain.ParseStatus(prev)
		if err != nil {
			return fmt.Errorf("notification %s: %w", id, err)
		}
		out.Previous = previous
		if previous.In(domain.CancellableFrom) {
			row, err := q.CancelNotification(ctx, sqlcgen.CancelNotificationParams{ID: uid, Now: at})
			if err != nil {
				return translate(err, "cancel")
			}
			out.Changed = true
			out.Notification, err = toDomain(row)
			return err
		}
		row, err := q.GetNotification(ctx, uid)
		if err != nil {
			return translate(err, "read notification")
		}
		out.Notification, err = toDomain(row)
		return err
	})
	if err != nil {
		return ports.CancelOutcome{}, err
	}
	return out, nil
}

// --- Arriving with later user stories ---------------------------------------------------------

func (r *NotificationRepo) List(context.Context, ports.ListFilter) ([]domain.Notification, error) {
	return nil, fmt.Errorf("list notifications (US6): %w", errNotYet)
}

func (r *NotificationRepo) BacklogCounts(context.Context) ([]ports.BacklogCount, error) {
	return nil, fmt.Errorf("backlog counts (US7): %w", errNotYet)
}

// --- Mapping ----------------------------------------------------------------------------------

func parseID(id string) (uuid.UUID, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("notification id %q: %w", id, domain.ErrNotFound)
	}
	return u, nil
}

func optionalID(id *string) (*uuid.UUID, error) {
	if id == nil {
		return nil, nil
	}
	u, err := uuid.Parse(*id)
	if err != nil {
		return nil, fmt.Errorf("id %q: %w", *id, err)
	}
	return &u, nil
}

func truncated(s *string) *string {
	if s == nil {
		return nil
	}
	t := domain.TruncateError(*s)
	return &t
}

func insertParams(n *domain.Notification) (sqlcgen.InsertNotificationParams, error) {
	id, err := uuid.Parse(n.ID)
	if err != nil {
		return sqlcgen.InsertNotificationParams{}, fmt.Errorf("notification id: %w", err)
	}
	batchID, err := optionalID(n.BatchID)
	if err != nil {
		return sqlcgen.InsertNotificationParams{}, err
	}
	templateID, err := optionalID(n.TemplateID)
	if err != nil {
		return sqlcgen.InsertNotificationParams{}, err
	}
	var vars []byte
	if n.TemplateVars != nil {
		if vars, err = json.Marshal(n.TemplateVars); err != nil {
			return sqlcgen.InsertNotificationParams{}, fmt.Errorf("template vars: %w", err)
		}
	}
	return sqlcgen.InsertNotificationParams{
		ID: id, BatchID: batchID, IdempotencyKey: n.IdempotencyKey, PayloadHash: n.PayloadHash,
		Channel: string(n.Channel), Recipient: n.Recipient, Content: n.Content, Priority: int16(n.Priority),
		Status: string(n.Status), MaxAttempts: int32(min(n.MaxAttempts, 1<<30)), //nolint:gosec // bounded
		NextAttemptAt: n.NextAttemptAt, ScheduledAt: n.ScheduledAt, TemplateID: templateID,
		TemplateVars: vars, CorrelationID: n.CorrelationID, CreatedAt: n.CreatedAt,
	}, nil
}

func toDomain(r sqlcgen.Notification) (*domain.Notification, error) {
	ch, err := domain.ParseChannel(r.Channel)
	if err != nil {
		return nil, fmt.Errorf("row %s: %w", r.ID, err)
	}
	st, err := domain.ParseStatus(r.Status)
	if err != nil {
		return nil, fmt.Errorf("row %s: %w", r.ID, err)
	}
	prio, err := domain.PriorityFromInt(r.Priority)
	if err != nil {
		return nil, fmt.Errorf("row %s: %w", r.ID, err)
	}
	n := &domain.Notification{
		ID: r.ID.String(), IdempotencyKey: r.IdempotencyKey, PayloadHash: r.PayloadHash,
		Channel: ch, Recipient: r.Recipient, Content: r.Content, Priority: prio, Status: st,
		Attempts: int(r.Attempts), MaxAttempts: int(r.MaxAttempts), NextAttemptAt: r.NextAttemptAt.UTC(),
		ScheduledAt: utc(r.ScheduledAt), ProviderMessageID: r.ProviderMessageID, LastError: r.LastError,
		CorrelationID: r.CorrelationID, CreatedAt: r.CreatedAt.UTC(), UpdatedAt: r.UpdatedAt.UTC(),
		SentAt: utc(r.SentAt), CancelledAt: utc(r.CancelledAt), FailedAt: utc(r.FailedAt),
	}
	if r.BatchID != nil {
		s := r.BatchID.String()
		n.BatchID = &s
	}
	if r.TemplateID != nil {
		s := r.TemplateID.String()
		n.TemplateID = &s
	}
	if len(r.TemplateVars) > 0 {
		if err := json.Unmarshal(r.TemplateVars, &n.TemplateVars); err != nil {
			return nil, fmt.Errorf("row %s template vars: %w", r.ID, err)
		}
	}
	return n, nil
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
