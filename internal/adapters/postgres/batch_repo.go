package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres/sqlcgen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// BatchRepo implements ports.BatchRepository on PostgreSQL.
type BatchRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

var _ ports.BatchRepository = (*BatchRepo)(nil)

// NewBatchRepo returns a repository on pool.
func NewBatchRepo(pool *pgxpool.Pool) *BatchRepo {
	return &BatchRepo{pool: pool, q: sqlcgen.New(pool)}
}

// CreateBatch writes the batch row, bulk-inserts its items in one statement (SC-002), resolves
// every idempotency-key conflict by payload hash (data-model.md notifications.idempotency_key,
// notifications.payload_hash), and stores the final totals, all in one transaction.
func (r *BatchRepo) CreateBatch(ctx context.Context, b domain.Batch, items []*domain.Notification) ([]ports.BatchItemOutcome, error) {
	batchID, err := uuid.Parse(b.ID)
	if err != nil {
		return nil, fmt.Errorf("batch id: %w", err)
	}
	params, err := bulkParams(batchID, b, items)
	if err != nil {
		return nil, err
	}
	out := make([]ports.BatchItemOutcome, len(items))
	err = WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		if err := q.InsertBatch(ctx, sqlcgen.InsertBatchParams{ID: batchID, CorrelationID: b.CorrelationID, CreatedAt: b.CreatedAt}); err != nil {
			return translate(err, "insert batch")
		}
		inserted, err := q.InsertNotificationsBulk(ctx, params)
		if err != nil {
			return translate(err, "bulk insert notifications")
		}
		created := make(map[uuid.UUID]bool, len(inserted))
		for _, id := range inserted {
			created[id] = true
		}

		var conflictKeys []string
		for i, n := range items {
			if created[params.Ids[i]] {
				stored := *n
				out[i] = ports.BatchItemOutcome{Kind: ports.BatchItemCreated, Stored: &stored}
				continue
			}
			if n.IdempotencyKey == nil {
				return fmt.Errorf("item %d was not inserted and has no idempotency key", i)
			}
			conflictKeys = append(conflictKeys, *n.IdempotencyKey)
		}
		if len(conflictKeys) > 0 {
			rows, err := q.ListByIdempotencyKeys(ctx, conflictKeys)
			if err != nil {
				return translate(err, "read existing keys")
			}
			byKey := make(map[string]*domain.Notification, len(rows))
			for _, row := range rows {
				n, err := toDomain(row)
				if err != nil {
					return err
				}
				byKey[*n.IdempotencyKey] = n
			}
			for i, n := range items {
				if out[i].Stored != nil {
					continue
				}
				existing, ok := byKey[*n.IdempotencyKey]
				if !ok {
					return fmt.Errorf("item %d: key %q conflicted but no row holds it", i, *n.IdempotencyKey)
				}
				kind := ports.BatchItemReplayed
				if !bytes.Equal(existing.PayloadHash, n.PayloadHash) {
					kind = ports.BatchItemConflict
				}
				out[i] = ports.BatchItemOutcome{Kind: kind, Stored: existing}
			}
		}

		accepted, rejected := 0, b.TotalRejected
		for _, o := range out {
			if o.Kind == ports.BatchItemConflict {
				rejected++
			} else {
				accepted++
			}
		}
		err = q.SetBatchTotals(ctx, sqlcgen.SetBatchTotalsParams{ID: batchID,
			TotalAccepted: int32(min(accepted, 1<<30)), TotalRejected: int32(min(rejected, 1<<30))}) //nolint:gosec // bounded by the batch size
		return translate(err, "set batch totals")
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetBatch returns the batch, its status counts and its notifications (FR-010).
func (r *BatchRepo) GetBatch(ctx context.Context, id string) (*domain.Batch, ports.BatchCounts, []domain.Notification, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("batch id %q: %w", id, domain.ErrNotFound)
	}
	row, err := r.q.GetBatch(ctx, uid)
	if err != nil {
		return nil, nil, nil, translate(err, "get batch")
	}
	b := &domain.Batch{ID: row.ID.String(), TotalAccepted: int(row.TotalAccepted), TotalRejected: int(row.TotalRejected),
		CorrelationID: row.CorrelationID, CreatedAt: row.CreatedAt.UTC()}

	counted, err := r.q.CountByStatus(ctx, &uid)
	if err != nil {
		return nil, nil, nil, translate(err, "count batch statuses")
	}
	counts := ports.BatchCounts{}
	for _, c := range counted {
		st, err := domain.ParseStatus(c.Status)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("batch %s: %w", id, err)
		}
		counts[st] = int(c.N)
	}

	rows, err := r.q.ListByBatch(ctx, &uid)
	if err != nil {
		return nil, nil, nil, translate(err, "list batch notifications")
	}
	items := make([]domain.Notification, 0, len(rows))
	for _, row := range rows {
		n, err := toDomain(row)
		if err != nil {
			return nil, nil, nil, err
		}
		items = append(items, *n)
	}
	return b, counts, items, nil
}

// CancelBatch cancels every cancellable notification of a batch in one transaction and counts
// the rest (FR-015). The items are locked first, so open relay transactions finish before the
// decision.
func (r *BatchRepo) CancelBatch(ctx context.Context, id string, at time.Time) (ports.BatchCancelResult, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return ports.BatchCancelResult{}, fmt.Errorf("batch id %q: %w", id, domain.ErrNotFound)
	}
	var res ports.BatchCancelResult
	err = WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		if _, err := q.GetBatch(ctx, uid); err != nil {
			return translate(err, "get batch")
		}
		locked, err := q.LockBatchItems(ctx, &uid)
		if err != nil {
			return translate(err, "lock batch items")
		}
		previous := make(map[uuid.UUID]domain.Status, len(locked))
		var ids []uuid.UUID
		for _, row := range locked {
			st, err := domain.ParseStatus(row.Status)
			if err != nil {
				return fmt.Errorf("batch %s: %w", id, err)
			}
			switch {
			case st == domain.StatusCancelled:
				res.AlreadyCancelled++
			case st.In(domain.CancellableFrom):
				previous[row.ID] = st
				ids = append(ids, row.ID)
			default:
				res.NotCancellable++
			}
		}
		if len(ids) == 0 {
			return nil
		}
		rows, err := q.CancelMany(ctx, sqlcgen.CancelManyParams{Ids: ids, Now: at})
		if err != nil {
			return translate(err, "cancel batch items")
		}
		for _, row := range rows {
			n, err := toDomain(row)
			if err != nil {
				return err
			}
			res.Rows = append(res.Rows, ports.CancelOutcome{Notification: n, Previous: previous[row.ID], Changed: true})
		}
		res.Cancelled = len(rows)
		return nil
	})
	if err != nil {
		return ports.BatchCancelResult{}, err
	}
	return res, nil
}

// bulkParams turns items into parallel arrays. Nullable columns use the sentinels documented in
// sql/queries/batches.sql: "" for absent text and the zero time for an absent schedule.
func bulkParams(batchID uuid.UUID, b domain.Batch, items []*domain.Notification) (sqlcgen.InsertNotificationsBulkParams, error) {
	n := len(items)
	p := sqlcgen.InsertNotificationsBulkParams{
		BatchID: batchID, CorrelationID: b.CorrelationID, CreatedAt: b.CreatedAt,
		Ids: make([]uuid.UUID, n), IdempotencyKeys: make([]string, n), PayloadHashes: make([][]byte, n),
		Channels: make([]string, n), Recipients: make([]string, n), Contents: make([]string, n),
		Priorities: make([]int16, n), Statuses: make([]string, n), MaxAttempts: make([]int32, n),
		NextAttemptAts: make([]time.Time, n), ScheduledAts: make([]time.Time, n),
		TemplateIds: make([]string, n), TemplateVars: make([]string, n),
	}
	for i, it := range items {
		id, err := uuid.Parse(it.ID)
		if err != nil {
			return p, fmt.Errorf("item %d id: %w", i, err)
		}
		p.Ids[i] = id
		if it.IdempotencyKey != nil {
			p.IdempotencyKeys[i] = *it.IdempotencyKey
		}
		p.PayloadHashes[i] = it.PayloadHash
		p.Channels[i], p.Recipients[i], p.Contents[i] = string(it.Channel), it.Recipient, it.Content
		p.Priorities[i], p.Statuses[i] = int16(it.Priority), string(it.Status)
		p.MaxAttempts[i] = int32(min(it.MaxAttempts, 1<<30)) //nolint:gosec // bounded
		p.NextAttemptAts[i] = it.NextAttemptAt
		if it.ScheduledAt != nil {
			p.ScheduledAts[i] = *it.ScheduledAt
		}
		if it.TemplateID != nil {
			p.TemplateIds[i] = *it.TemplateID
		}
		if it.TemplateVars != nil {
			raw, err := json.Marshal(it.TemplateVars)
			if err != nil {
				return p, fmt.Errorf("item %d template vars: %w", i, err)
			}
			p.TemplateVars[i] = string(raw)
		}
	}
	return p, nil
}
