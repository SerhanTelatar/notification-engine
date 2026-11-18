package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

func TestGetNotificationWithAttemptHistory(t *testing.T) {
	repo := newRepo()
	clock := newClock()
	n := queuedNotification("n1", domain.ChannelSMS, clock.Now())
	n.Status, n.Attempts = domain.StatusSent, 2
	repo.put(n)
	status503, status202 := 503, 202
	errText := "provider returned HTTP 503"
	// Stored out of order: the use case must return them by attempt number.
	repo.attempts["n1"] = []domain.Attempt{
		{NotificationID: "n1", AttemptNo: 2, Outcome: domain.OutcomeSuccess, HTTPStatus: &status202, StartedAt: clock.Now().Add(2 * time.Second)},
		{NotificationID: "n1", AttemptNo: 1, Outcome: domain.OutcomeTransient, HTTPStatus: &status503, Error: &errText, StartedAt: clock.Now()},
	}

	q := NewQuery(QueryDeps{Repo: repo})
	got, err := q.GetNotification(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Notification.ID != "n1" || got.Notification.Status != domain.StatusSent || got.Notification.Attempts != 2 {
		t.Fatalf("notification %+v", got.Notification)
	}
	if len(got.Attempts) != 2 || got.Attempts[0].AttemptNo != 1 || got.Attempts[1].AttemptNo != 2 {
		t.Fatalf("attempts %+v, want ordered by attempt_no", got.Attempts)
	}
	if got.Attempts[0].Error == nil || *got.Attempts[0].Error != errText || got.Attempts[0].Outcome != domain.OutcomeTransient {
		t.Fatalf("first attempt %+v", got.Attempts[0])
	}
}

func TestGetNotificationWithoutAttempts(t *testing.T) {
	repo := newRepo()
	repo.put(queuedNotification("fresh", domain.ChannelEmail, newClock().Now()))
	got, err := NewQuery(QueryDeps{Repo: repo}).GetNotification(context.Background(), "fresh")
	if err != nil || got.Attempts == nil || len(got.Attempts) != 0 {
		t.Fatalf("got %+v err %v: a notification with no attempts has an empty (non-nil) history", got, err)
	}
}

func TestGetNotificationNotFound(t *testing.T) {
	_, err := NewQuery(QueryDeps{Repo: newRepo()}).GetNotification(context.Background(), "missing")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// listRepo records the filter and returns canned rows.
type listRepo struct {
	*memRepo
	got  ports.ListFilter
	rows []domain.Notification
}

func (r *listRepo) List(_ context.Context, f ports.ListFilter) ([]domain.Notification, error) {
	r.got = f
	if len(r.rows) > f.Limit {
		return r.rows[:f.Limit], nil
	}
	return r.rows, nil
}

func TestListPagingAndValidation(t *testing.T) {
	base := newClock().Now()
	var rows []domain.Notification
	for i := range 3 {
		n := queuedNotification(fmt.Sprintf("00000000-0000-7000-8000-00000000000%d", i), domain.ChannelSMS, base.Add(-time.Duration(i)*time.Second))
		rows = append(rows, *n)
	}
	repo := &listRepo{memRepo: newRepo(), rows: rows}
	q := NewQuery(QueryDeps{Repo: repo})

	limit := 2
	res, err := q.List(context.Background(), ListRequest{Limit: &limit, Status: strp("queued"), Channel: strp("sms")})
	if err != nil {
		t.Fatal(err)
	}
	if repo.got.Limit != 3 || *repo.got.Status != domain.StatusQueued || *repo.got.Channel != domain.ChannelSMS {
		t.Fatalf("filter %+v: must fetch limit+1 with parsed filters", repo.got)
	}
	if len(res.Items) != 2 || res.NextCursor == nil {
		t.Fatalf("page %d items next=%v, want 2 and a cursor", len(res.Items), res.NextCursor)
	}
	c, err := DecodeCursor(*res.NextCursor)
	if err != nil || c.ID != rows[1].ID || !c.CreatedAt.Equal(rows[1].CreatedAt) {
		t.Fatalf("cursor %+v must point at the page's last row", c)
	}

	res, _ = q.List(context.Background(), ListRequest{})
	if repo.got.Limit != DefaultPageSize+1 || res.NextCursor != nil || len(res.Items) != 3 {
		t.Fatalf("default page: limit %d next %v items %d", repo.got.Limit, res.NextCursor, len(res.Items))
	}

	tooBig, from, to := 101, base, base.Add(-time.Hour)
	_, err = q.List(context.Background(), ListRequest{Limit: &tooBig, Status: strp("delivered"), Channel: strp("fax"),
		From: &from, To: &to, Cursor: strp("garbage")})
	var v *domain.ValidationError
	if !errors.As(err, &v) || len(v.Fields) != 5 {
		t.Fatalf("err %v: want one ValidationError with limit, status, channel, to and cursor", err)
	}
}
