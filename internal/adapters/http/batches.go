package httpapi

import (
	"context"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
)

// CreateBatch accepts 1–1,000 notifications with per-item results (FR-007, FR-008). It returns
// 202 when at least one item is accepted; otherwise the use case returns a ValidationError (400)
// listing every item's problems.
func (s *Server) CreateBatch(ctx context.Context, req gen.CreateBatchRequestObject) (gen.CreateBatchResponseObject, error) {
	if s.batch == nil {
		return nil, errNotImplemented
	}
	if req.Body == nil {
		return nil, domain.NewFieldError("body", "is required")
	}
	cid, traceParent := logger.CorrelationID(ctx), tracing.TraceParent(ctx)
	items := make([]app.CreateRequest, len(req.Body.Items))
	for i := range req.Body.Items {
		it := &req.Body.Items[i]
		items[i] = fromAPIRequest(it, it.IdempotencyKey, cid)
		items[i].TraceParent = traceParent
	}
	res, err := s.batch.CreateBatch(ctx, app.BatchRequest{Items: items, CorrelationID: cid})
	if err != nil {
		s.countRejected(err)
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	s.countBatch(res.Items)

	out := gen.BatchCreateResult{BatchId: parseUUID(res.BatchID), Accepted: res.Accepted, Rejected: res.Rejected,
		Items: make([]gen.BatchItemResult, len(res.Items))}
	for i, r := range res.Items {
		item := gen.BatchItemResult{Index: r.Index, Accepted: r.Accepted}
		if r.Notification != nil && r.Accepted {
			id, st := parseUUID(r.Notification.ID), gen.Status(r.Notification.Status)
			item.NotificationId, item.Status = &id, &st
		}
		if len(r.Errors) > 0 {
			errs := make([]gen.FieldError, len(r.Errors))
			for j, fe := range r.Errors {
				errs[j] = gen.FieldError{Field: fe.Field, Message: fe.Message}
			}
			item.Errors = &errs
		}
		out.Items[i] = item
	}
	return gen.CreateBatch202JSONResponse(out), nil
}

// CancelBatch cancels every cancellable item of a batch and reports the counts (FR-015).
func (s *Server) CancelBatch(ctx context.Context, req gen.CancelBatchRequestObject) (gen.CancelBatchResponseObject, error) {
	if s.cancel == nil {
		return nil, errNotImplemented
	}
	res, err := s.cancel.CancelBatch(ctx, req.Id.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	return gen.CancelBatch200JSONResponse(gen.BatchCancelResult{BatchId: req.Id, Cancelled: res.Cancelled,
		AlreadyCancelled: res.AlreadyCancelled, NotCancellable: res.NotCancellable}), nil
}

// GetBatch returns status counts and the batch's notifications (FR-010, spec US2-AS3).
func (s *Server) GetBatch(ctx context.Context, req gen.GetBatchRequestObject) (gen.GetBatchResponseObject, error) {
	if s.batch == nil {
		return nil, errNotImplemented
	}
	d, err := s.batch.GetBatch(ctx, req.Id.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	counts := make(map[string]int, len(d.Counts))
	for st, n := range d.Counts {
		counts[string(st)] = n
	}
	items := make([]gen.Notification, len(d.Items))
	for i := range d.Items {
		items[i] = toAPINotification(&d.Items[i])
	}
	return gen.GetBatch200JSONResponse(gen.BatchDetail{
		BatchId: parseUUID(d.Batch.ID), Total: d.Batch.TotalAccepted, Counts: counts,
		Items: items, CreatedAt: d.Batch.CreatedAt,
	}), nil
}
