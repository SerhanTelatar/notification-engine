package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// MaxBatchItems is the batch size limit (FR-007).
const MaxBatchItems = 1000

// BatchRequest is a batch as submitted. Every item inherits the request's correlation id.
type BatchRequest struct {
	Items         []CreateRequest
	CorrelationID string
}

// BatchItemResult is the outcome of one item, at its index in the request (FR-008).
type BatchItemResult struct {
	Index        int
	Accepted     bool
	Replayed     bool // an idempotency key resolved to an existing notification
	Notification *domain.Notification
	Errors       []domain.FieldError
}

// BatchResult is the response to a batch submission.
type BatchResult struct {
	BatchID  string
	Accepted int
	Rejected int
	Items    []BatchItemResult
}

// BatchDetail is a batch with its status counts and notifications (FR-010).
type BatchDetail struct {
	Batch  *domain.Batch
	Counts ports.BatchCounts
	Items  []domain.Notification
}

// BatcherDeps are the Batcher's collaborators.
type BatcherDeps struct {
	Repo        ports.BatchRepository
	IDs         ports.IDGenerator
	Clock       ports.Clock
	Events      ports.StatusBroadcaster
	Log         *slog.Logger
	MaxAttempts int
}

// Batcher implements CreateBatch and GetBatch (US4).
type Batcher struct{ d BatcherDeps }

// NewBatcher returns the batch use cases.
func NewBatcher(d BatcherDeps) *Batcher { return &Batcher{d: d} }

// KeyConflictMessage is the item error for an idempotency key reused with a different payload.
const KeyConflictMessage = "already used with a different payload"

// CreateBatch validates every item on its own and stores the valid ones as one batch (partial
// success, FR-008). If no item is valid, nothing is stored and the error lists every item's
// problems under items[i].field.
func (b *Batcher) CreateBatch(ctx context.Context, req BatchRequest) (BatchResult, error) {
	if n := len(req.Items); n == 0 || n > MaxBatchItems {
		return BatchResult{}, domain.NewFieldError("items", fmt.Sprintf("must contain 1 to %d items (got %d)", MaxBatchItems, n))
	}
	batchID := b.d.IDs.NewID()
	now := b.d.Clock.Now()

	results := make([]BatchItemResult, len(req.Items))
	var (
		candidates []*domain.Notification // unique items to store, in request order
		candIndex  []int                  // request index of each candidate
		aliasOf    = map[int]int{}        // request index -> candidate position (in-batch duplicate key)
		firstByKey = map[string]int{}     // idempotency key -> candidate position
		all        domain.ValidationError
	)
	reject := func(i int, errs []domain.FieldError) {
		results[i] = BatchItemResult{Index: i, Errors: errs}
		for _, fe := range errs {
			all.Add(fmt.Sprintf("items[%d].%s", i, fe.Field), fe.Message)
		}
	}
	for i, item := range req.Items {
		item.CorrelationID = req.CorrelationID
		n, err := buildNotification(item, b.d.IDs.NewID(), now, b.d.MaxAttempts, &batchID)
		if err != nil {
			var v *domain.ValidationError
			if !errors.As(err, &v) {
				return BatchResult{}, fmt.Errorf("item %d: %w", i, err)
			}
			reject(i, v.Fields)
			continue
		}
		if n.IdempotencyKey != nil {
			if pos, seen := firstByKey[*n.IdempotencyKey]; seen {
				if bytes.Equal(candidates[pos].PayloadHash, n.PayloadHash) {
					aliasOf[i] = pos // same request twice in one batch: one notification
				} else {
					reject(i, []domain.FieldError{{Field: "idempotency_key", Message: KeyConflictMessage + " earlier in this batch"}})
				}
				continue
			}
			firstByKey[*n.IdempotencyKey] = len(candidates)
		}
		candidates = append(candidates, n)
		candIndex = append(candIndex, i)
	}
	if len(candidates) == 0 {
		return BatchResult{}, &all
	}

	rejected := len(req.Items) - len(candidates) - len(aliasOf)
	batch := domain.Batch{ID: batchID, CorrelationID: req.CorrelationID, CreatedAt: now, TotalRejected: rejected}
	outcomes, err := b.d.Repo.CreateBatch(ctx, batch, candidates)
	if err != nil {
		return BatchResult{}, fmt.Errorf("store batch: %w", err)
	}

	for pos, o := range outcomes {
		i := candIndex[pos]
		switch o.Kind {
		case ports.BatchItemCreated:
			results[i] = BatchItemResult{Index: i, Accepted: true, Notification: o.Stored}
			broadcast(ctx, b.d.Log, b.d.Events, o.Stored, nil, now)
		case ports.BatchItemReplayed:
			results[i] = BatchItemResult{Index: i, Accepted: true, Replayed: true, Notification: o.Stored}
		case ports.BatchItemConflict:
			results[i] = BatchItemResult{Index: i, Errors: []domain.FieldError{{Field: "idempotency_key", Message: KeyConflictMessage}}}
		}
	}
	for i, pos := range aliasOf {
		first := results[candIndex[pos]]
		results[i] = BatchItemResult{Index: i, Accepted: first.Accepted, Replayed: first.Accepted,
			Notification: first.Notification, Errors: first.Errors}
	}

	res := BatchResult{BatchID: batchID, Items: results}
	for _, r := range results {
		if r.Accepted {
			res.Accepted++
		} else {
			res.Rejected++
		}
	}
	b.d.Log.InfoContext(ctx, "batch accepted", "batch_id", batchID, "accepted", res.Accepted, "rejected", res.Rejected)
	return res, nil
}

// GetBatch returns the batch with its status counts and notifications.
func (b *Batcher) GetBatch(ctx context.Context, id string) (BatchDetail, error) {
	batch, counts, items, err := b.d.Repo.GetBatch(ctx, id)
	if err != nil {
		return BatchDetail{}, fmt.Errorf("get batch %s: %w", id, err)
	}
	if items == nil {
		items = []domain.Notification{}
	}
	return BatchDetail{Batch: batch, Counts: counts, Items: items}, nil
}
