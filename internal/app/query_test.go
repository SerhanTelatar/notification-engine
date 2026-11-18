package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
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
