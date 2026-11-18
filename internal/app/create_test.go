package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

type createFixture struct {
	repo   *memRepo
	events *recBroadcaster
	clock  *fakeClock
	uc     *Creator
}

func newCreateFixture() createFixture {
	f := createFixture{repo: newRepo(), events: &recBroadcaster{}, clock: newClock()}
	f.uc = NewCreator(CreatorDeps{
		Repo: f.repo, IDs: &seqIDs{}, Clock: f.clock, Events: f.events, Log: quietLog, MaxAttempts: 5,
	})
	return f
}

func smsRequest() CreateRequest {
	return CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: strp("Your code is 1234"), CorrelationID: "c-1"}
}

func TestCreateAcceptsValidRequest(t *testing.T) {
	future := newClock().Now().Add(10 * time.Minute)
	tests := []struct {
		name       string
		mut        func(*CreateRequest)
		wantStatus domain.Status
	}{
		{"immediate is pending", func(*CreateRequest) {}, domain.StatusPending},
		{"future schedule is scheduled", func(r *CreateRequest) { r.ScheduledAt = &future }, domain.StatusScheduled},
		{"high priority email", func(r *CreateRequest) {
			r.Channel, r.Recipient, r.Priority = "email", "ada@example.com", "high"
		}, domain.StatusPending},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateFixture()
			req := smsRequest()
			tc.mut(&req)
			res, err := f.uc.Create(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			n := res.Notification
			if res.Replayed || n.Status != tc.wantStatus || n.CorrelationID != "c-1" || n.MaxAttempts != 5 {
				t.Fatalf("unexpected result %+v (replayed=%v)", n, res.Replayed)
			}
			if f.repo.get(n.ID) == nil {
				t.Fatal("notification not persisted")
			}
			if got := f.events.statuses(n.ID); len(got) != 1 || got[0] != tc.wantStatus {
				t.Fatalf("broadcast %v, want one %s event after commit", got, tc.wantStatus)
			}
		})
	}
}

func TestCreateCollectsEveryValidationError(t *testing.T) {
	f := newCreateFixture()
	req := smsRequest()
	req.Recipient = "12345"
	req.Content = strp(strings.Repeat("a", 161))
	req.Priority = "urgent"

	_, err := f.uc.Create(context.Background(), req)
	var v *domain.ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	fields := map[string]bool{}
	for _, fe := range v.Fields {
		fields[fe.Field] = true
	}
	for _, want := range []string{"recipient", "content", "priority"} {
		if !fields[want] {
			t.Errorf("missing %s in %v", want, v.Fields)
		}
	}
	if len(f.repo.rows) != 0 || len(f.events.events) != 0 {
		t.Fatal("nothing may be stored or broadcast for an invalid request")
	}
}

func TestCreateContentSource(t *testing.T) {
	tid := "0193a3b0-0000-7000-8000-000000000001"
	tests := []struct {
		name      string
		mut       func(*CreateRequest)
		wantField string
	}{
		{"neither content nor template", func(r *CreateRequest) { r.Content = nil }, "content"},
		{"both content and template", func(r *CreateRequest) { r.TemplateID = &tid }, "content"},
		{"template before US8 is unknown", func(r *CreateRequest) { r.Content, r.TemplateID = nil, &tid }, "template_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateFixture()
			req := smsRequest()
			tc.mut(&req)
			_, err := f.uc.Create(context.Background(), req)
			var v *domain.ValidationError
			if !errors.As(err, &v) || v.Fields[0].Field != tc.wantField {
				t.Fatalf("want validation error on %s, got %v", tc.wantField, err)
			}
		})
	}
}

func TestCreateIdempotency(t *testing.T) {
	f := newCreateFixture()
	ctx := context.Background()
	req := smsRequest()
	req.IdempotencyKey = strp("order-42")

	first, err := f.uc.Create(ctx, req)
	if err != nil || first.Replayed {
		t.Fatalf("first create: %+v %v", first, err)
	}

	// Same key, same payload: the original comes back, nothing new is stored or broadcast.
	again := req
	again.CorrelationID = "c-retry" // a client retry carries a new correlation id
	second, err := f.uc.Create(ctx, again)
	if err != nil || !second.Replayed || second.Notification.ID != first.Notification.ID {
		t.Fatalf("replay: %+v %v", second, err)
	}
	if len(f.repo.rows) != 1 || len(f.events.events) != 1 {
		t.Fatalf("replay must not create or broadcast: rows=%d events=%d", len(f.repo.rows), len(f.events.events))
	}

	// Same key, different payload: conflict, original untouched.
	changed := req
	changed.Content = strp("Your code is 9999")
	if _, err := f.uc.Create(ctx, changed); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
	if f.repo.get(first.Notification.ID).Content != "Your code is 1234" {
		t.Fatal("conflicting request must not modify the original")
	}
}

func TestCreateFailuresAndBestEffortEvents(t *testing.T) {
	t.Run("repository error propagates", func(t *testing.T) {
		f := newCreateFixture()
		f.repo.createErr = errors.New("connection reset")
		if _, err := f.uc.Create(context.Background(), smsRequest()); err == nil || !strings.Contains(err.Error(), "connection reset") {
			t.Fatalf("got %v", err)
		}
		if len(f.events.events) != 0 {
			t.Fatal("no event without a commit")
		}
	})
	t.Run("broadcast failure does not fail the request", func(t *testing.T) {
		f := newCreateFixture()
		f.events.err = errors.New("redis down")
		if _, err := f.uc.Create(context.Background(), smsRequest()); err != nil {
			t.Fatalf("best-effort broadcast must not fail create: %v", err)
		}
	})
}
