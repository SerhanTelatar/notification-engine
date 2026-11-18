package app

import (
	"context"
	"fmt"
	"sort"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// QueryDeps are the read side's collaborators.
type QueryDeps struct {
	Repo ports.NotificationRepository
}

// Query implements the read use cases (US2 status lookup; list arrives with US6).
type Query struct{ d QueryDeps }

// NewQuery returns the read use cases.
func NewQuery(d QueryDeps) *Query { return &Query{d: d} }

// NotificationDetail is one notification with its attempt history (FR-009).
type NotificationDetail struct {
	Notification *domain.Notification
	Attempts     []domain.Attempt // ordered by attempt number, never nil
}

// GetNotification returns the notification and its attempts. An unknown id is domain.ErrNotFound.
func (q *Query) GetNotification(ctx context.Context, id string) (NotificationDetail, error) {
	n, err := q.d.Repo.Get(ctx, id)
	if err != nil {
		return NotificationDetail{}, fmt.Errorf("get notification %s: %w", id, err)
	}
	attempts, err := q.d.Repo.ListAttempts(ctx, id)
	if err != nil {
		return NotificationDetail{}, fmt.Errorf("list attempts of %s: %w", id, err)
	}
	if attempts == nil {
		attempts = []domain.Attempt{}
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].AttemptNo < attempts[j].AttemptNo })
	return NotificationDetail{Notification: n, Attempts: attempts}, nil
}
