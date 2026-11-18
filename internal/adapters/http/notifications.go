package httpapi

import (
	"context"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
)

const replayedHeaderValue = "true"

// CreateNotification accepts one notification (FR-001/002): 202 once it's stored, with delivery
// left to the worker. Errors are domain errors, mapped by responseErrorHandler.
func (s *Server) CreateNotification(ctx context.Context, req gen.CreateNotificationRequestObject) (gen.CreateNotificationResponseObject, error) {
	if s.create == nil {
		return nil, errNotImplemented
	}
	if req.Body == nil {
		return nil, domain.NewFieldError("body", "is required")
	}
	b := req.Body
	key, err := mergeIdempotencyKey(req.Params.IdempotencyKey, b.IdempotencyKey)
	if err != nil {
		s.countRejected(err)
		return nil, err
	}

	cr := fromAPIRequest(b, key, logger.CorrelationID(ctx))
	cr.TraceParent = tracing.TraceParent(ctx) // "" when tracing is off
	res, err := s.create.Create(ctx, cr)
	if err != nil {
		s.countRejected(err)
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	if !res.Replayed {
		s.countCreated(res.Notification)
	}
	cid := logger.CorrelationID(ctx)
	headers := gen.CreateNotification202ResponseHeaders{XCorrelationID: &cid}
	if res.Replayed {
		v := replayedHeaderValue
		headers.IdempotentReplayed = &v
	}
	return gen.CreateNotification202JSONResponse{Body: toAPINotification(res.Notification), Headers: headers}, nil
}

// GetNotification returns one notification with its attempt history (FR-009).
func (s *Server) GetNotification(ctx context.Context, req gen.GetNotificationRequestObject) (gen.GetNotificationResponseObject, error) {
	if s.query == nil {
		return nil, errNotImplemented
	}
	d, err := s.query.GetNotification(ctx, req.Id.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	return gen.GetNotification200JSONResponse(toAPIDetail(d)), nil
}

// ListNotifications returns one page of notifications, newest first, with keyset pagination
// (FR-011). Parameter problems are a 400 listing every invalid field.
func (s *Server) ListNotifications(ctx context.Context, req gen.ListNotificationsRequestObject) (gen.ListNotificationsResponseObject, error) {
	if s.query == nil {
		return nil, errNotImplemented
	}
	p := req.Params
	lr := app.ListRequest{From: p.From, To: p.To, Limit: p.Limit, Cursor: p.Cursor}
	if p.Status != nil {
		st := string(*p.Status)
		lr.Status = &st
	}
	if p.Channel != nil {
		ch := string(*p.Channel)
		lr.Channel = &ch
	}
	res, err := s.query.List(ctx, lr)
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	items := make([]gen.Notification, len(res.Items))
	for i := range res.Items {
		items[i] = toAPINotification(&res.Items[i])
	}
	return gen.ListNotifications200JSONResponse(gen.NotificationPage{Items: items, NextCursor: res.NextCursor}), nil
}

// CancelNotification cancels one notification (FR-013): 200 with the cancelled notification
// (also when it was already cancelled), 409 with current_status when it can't be, 404 when
// unknown.
func (s *Server) CancelNotification(ctx context.Context, req gen.CancelNotificationRequestObject) (gen.CancelNotificationResponseObject, error) {
	if s.cancel == nil {
		return nil, errNotImplemented
	}
	n, err := s.cancel.Cancel(ctx, req.Id.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	return gen.CancelNotification200JSONResponse(toAPINotification(n)), nil
}

// fromAPIRequest maps one NotificationRequest (single create or batch item) to the use case.
func fromAPIRequest(b *gen.NotificationRequest, key *string, correlationID string) app.CreateRequest {
	cr := app.CreateRequest{
		Recipient:      b.Recipient,
		Channel:        string(b.Channel),
		Content:        b.Content,
		ScheduledAt:    b.ScheduledAt,
		IdempotencyKey: key,
		CorrelationID:  correlationID,
	}
	if b.Priority != nil {
		cr.Priority = string(*b.Priority)
	}
	if b.TemplateId != nil {
		id := b.TemplateId.String()
		cr.TemplateID = &id
	}
	if b.Variables != nil {
		cr.Variables = *b.Variables
	}
	return cr
}

// mergeIdempotencyKey accepts the key from the Idempotency-Key header or the body field. Both are
// allowed only when they're equal (research R7).
func mergeIdempotencyKey(header, body *string) (*string, error) {
	switch {
	case header == nil:
		return body, nil
	case body == nil:
		return header, nil
	case *header == *body:
		return header, nil
	default:
		return nil, domain.NewFieldError("idempotency_key", "differs from the Idempotency-Key header; send one or make them equal")
	}
}

// toAPINotification maps the aggregate to the contract's Notification schema.
func toAPINotification(n *domain.Notification) gen.Notification {
	maxAttempts := n.MaxAttempts
	out := gen.Notification{
		Id:                parseUUID(n.ID),
		Status:            gen.Status(n.Status),
		Channel:           gen.Channel(n.Channel),
		Recipient:         n.Recipient,
		Priority:          gen.Priority(n.Priority.String()),
		Attempts:          n.Attempts,
		MaxAttempts:       &maxAttempts,
		ProviderMessageId: n.ProviderMessageID,
		LastError:         n.LastError,
		IdempotencyKey:    n.IdempotencyKey,
		CorrelationId:     n.CorrelationID,
		ScheduledAt:       n.ScheduledAt,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
		SentAt:            n.SentAt,
	}
	if n.BatchID != nil {
		id := parseUUID(*n.BatchID)
		out.BatchId = &id
	}
	if n.TemplateID != nil {
		id := parseUUID(*n.TemplateID)
		out.TemplateId = &id
	}
	return out
}

// parseUUID converts a stored id; ids originate from the database, so they're always valid.
func parseUUID(s string) uuid.UUID {
	u, _ := uuid.Parse(s)
	return u
}

// toAPIDetail maps a notification plus its attempts to the NotificationDetail schema.
func toAPIDetail(d app.NotificationDetail) gen.NotificationDetail {
	n := toAPINotification(d.Notification)
	out := gen.NotificationDetail{
		Id: n.Id, BatchId: n.BatchId, Status: n.Status, Channel: n.Channel, Recipient: n.Recipient,
		Content: d.Notification.Content, Priority: n.Priority, Attempts: n.Attempts, MaxAttempts: n.MaxAttempts,
		ProviderMessageId: n.ProviderMessageId, LastError: n.LastError, TemplateId: n.TemplateId,
		IdempotencyKey: n.IdempotencyKey, CorrelationId: n.CorrelationId, ScheduledAt: n.ScheduledAt,
		CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt, SentAt: n.SentAt,
		AttemptHistory: make([]gen.Attempt, len(d.Attempts)),
	}
	for i, a := range d.Attempts {
		finished, latency := a.FinishedAt, a.LatencyMS
		out.AttemptHistory[i] = gen.Attempt{
			AttemptNo: a.AttemptNo, StartedAt: a.StartedAt, FinishedAt: &finished, LatencyMs: &latency,
			HttpStatus: a.HTTPStatus, Outcome: gen.AttemptOutcome(a.Outcome), Error: a.Error,
		}
	}
	return out
}
