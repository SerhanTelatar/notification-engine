package httpapi

import (
	"context"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
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
		return nil, err
	}

	cr := app.CreateRequest{
		Recipient:      b.Recipient,
		Channel:        string(b.Channel),
		Content:        b.Content,
		ScheduledAt:    b.ScheduledAt,
		IdempotencyKey: key,
		CorrelationID:  logger.CorrelationID(ctx),
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

	res, err := s.create.Create(ctx, cr)
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	cid := logger.CorrelationID(ctx)
	headers := gen.CreateNotification202ResponseHeaders{XCorrelationID: &cid}
	if res.Replayed {
		v := replayedHeaderValue
		headers.IdempotentReplayed = &v
	}
	return gen.CreateNotification202JSONResponse{Body: toAPINotification(res.Notification), Headers: headers}, nil
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
