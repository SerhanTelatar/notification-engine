package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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

// List paging limits (FR-011).
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// ListRequest is GET /notifications as received; nil means "not given".
type ListRequest struct {
	Status  *string
	Channel *string
	From    *time.Time // inclusive
	To      *time.Time // exclusive
	Limit   *int
	Cursor  *string
}

// ListResult is one page, newest first. NextCursor is nil on the last page.
type ListResult struct {
	Items      []domain.Notification
	NextCursor *string
}

// List returns one page of notifications filtered by status, channel and created-at range, newest
// first, with keyset pagination (FR-011, research R13). Every invalid parameter is reported at once.
func (q *Query) List(ctx context.Context, req ListRequest) (ListResult, error) {
	verr := &domain.ValidationError{}
	f := ports.ListFilter{From: req.From, To: req.To, Limit: DefaultPageSize}
	if req.Limit != nil {
		if *req.Limit < 1 || *req.Limit > MaxPageSize {
			verr.Add("limit", fmt.Sprintf("must be between 1 and %d", MaxPageSize))
		} else {
			f.Limit = *req.Limit
		}
	}
	if req.Status != nil {
		st, err := domain.ParseStatus(*req.Status)
		if err != nil {
			verr.Add("status", "must be one of pending, scheduled, queued, sending, sent, retrying, failed, cancelled")
		} else {
			f.Status = &st
		}
	}
	if req.Channel != nil {
		ch, err := domain.ParseChannel(*req.Channel)
		if err != nil {
			verr.Add("channel", "must be one of sms, email, push")
		} else {
			f.Channel = &ch
		}
	}
	if req.From != nil && req.To != nil && !req.From.Before(*req.To) {
		verr.Add("to", "must be after from")
	}
	if req.Cursor != nil {
		c, err := DecodeCursor(*req.Cursor)
		var cv *domain.ValidationError
		switch {
		case errors.As(err, &cv):
			verr.Merge("", cv)
		case err != nil:
			return ListResult{}, err
		default:
			f.After = &c
		}
	}
	if err := verr.OrNil(); err != nil {
		return ListResult{}, err
	}

	limit := f.Limit
	f.Limit = limit + 1 // one extra row tells us whether there's a next page
	rows, err := q.d.Repo.List(ctx, f)
	if err != nil {
		return ListResult{}, fmt.Errorf("list notifications: %w", err)
	}
	res := ListResult{Items: rows}
	if len(rows) > limit {
		res.Items = rows[:limit]
		last := res.Items[limit-1]
		next := EncodeCursor(ports.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
		res.NextCursor = &next
	}
	if res.Items == nil {
		res.Items = []domain.Notification{}
	}
	return res, nil
}
