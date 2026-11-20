package domain

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var t0 = time.Date(2026, 11, 18, 10, 0, 0, 123456789, time.UTC)

func validDraft() Draft {
	return Draft{Recipient: "+905551234567", Channel: "sms", Content: "Your code is 1234", CorrelationID: "c-1"}
}

func TestNewNotificationInitialState(t *testing.T) {
	future := t0.Add(10 * time.Minute)
	past := t0.Add(-time.Minute)
	tests := []struct {
		name          string
		mut           func(*Draft)
		wantStatus    Status
		wantNext      time.Time
		wantScheduled bool
		wantPriority  Priority
	}{
		{"immediate", func(*Draft) {}, StatusPending, t0, false, PriorityNormal},
		{"future schedule", func(d *Draft) { d.ScheduledAt = &future }, StatusScheduled, future, true, PriorityNormal},
		{"past schedule means now", func(d *Draft) { d.ScheduledAt = &past }, StatusPending, t0, false, PriorityNormal},
		{"explicit high", func(d *Draft) { d.Priority = "high" }, StatusPending, t0, false, PriorityHigh},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := validDraft()
			tc.mut(&d)
			n, err := NewNotification(d, "id-1", t0, 5)
			if err != nil {
				t.Fatal(err)
			}
			if n.Status != tc.wantStatus || n.Priority != tc.wantPriority || n.MaxAttempts != 5 || n.Attempts != 0 {
				t.Fatalf("got %+v", n)
			}
			if !n.NextAttemptAt.Equal(tc.wantNext.Truncate(time.Microsecond)) {
				t.Fatalf("next_attempt_at = %v, want %v", n.NextAttemptAt, tc.wantNext)
			}
			if (n.ScheduledAt != nil) != tc.wantScheduled {
				t.Fatalf("scheduled_at = %v", n.ScheduledAt)
			}
			if n.CreatedAt.Nanosecond()%1000 != 0 {
				t.Fatal("timestamps must be truncated to microseconds")
			}
		})
	}
}

func TestNewNotificationCollectsAllErrors(t *testing.T) {
	long := strings.Repeat("k", 300)
	d := Draft{Recipient: "12345", Channel: "sms", Content: strings.Repeat("a", 161), Priority: "urgent", IdempotencyKey: &long}
	_, err := NewNotification(d, "id", t0, 5)
	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	got := map[string]bool{}
	for _, f := range v.Fields {
		got[f.Field] = true
	}
	for _, f := range []string{"recipient", "content", "priority", "idempotency_key"} {
		if !got[f] {
			t.Errorf("missing field error for %s in %v", f, v.Fields)
		}
	}

	// An unknown channel can't check channel rules, but still reports the channel.
	_, err = NewNotification(Draft{Channel: "fax", Recipient: "x", Content: "y"}, "id", t0, 5)
	if !errors.As(err, &v) || len(v.Fields) != 1 || v.Fields[0].Field != "channel" {
		t.Fatalf("want only a channel error, got %v", err)
	}

	// Blank fields are reported even when the channel is unknown: required-ness doesn't depend
	// on the channel.
	_, err = NewNotification(Draft{Channel: "fax", Recipient: " ", Content: ""}, "id", t0, 5)
	if !errors.As(err, &v) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	got = map[string]bool{}
	for _, f := range v.Fields {
		got[f.Field] = true
	}
	for _, f := range []string{"channel", "recipient", "content"} {
		if !got[f] {
			t.Errorf("missing field error for %s in %v", f, v.Fields)
		}
	}
}

func TestPayloadHash(t *testing.T) {
	base := validDraft()
	h0, _ := PayloadHash(base)

	same := base
	same.CorrelationID = "different-correlation" // not part of the payload
	same.Priority = "normal"                     // explicit default equals implicit default
	if h, _ := PayloadHash(same); !bytes.Equal(h, h0) {
		t.Fatal("hash must ignore correlation id and treat '' as normal")
	}

	tid := "0193a3b0-0000-7000-8000-000000000001"
	a := Draft{Channel: "sms", Recipient: "+12", TemplateID: &tid, Variables: map[string]string{"A": "1", "B": "2"}}
	b := Draft{Channel: "sms", Recipient: "+12", TemplateID: &tid, Variables: map[string]string{"B": "2", "A": "1"}}
	ha, _ := PayloadHash(a)
	hb, _ := PayloadHash(b)
	if !bytes.Equal(ha, hb) {
		t.Fatal("hash must be stable under variable key order")
	}

	sched := t0.Add(time.Hour)
	changes := []func(*Draft){
		func(d *Draft) { d.Recipient = "+905550000000" },
		func(d *Draft) { d.Channel = "push" },
		func(d *Draft) { d.Content = "Your code is 9999" },
		func(d *Draft) { d.Priority = "high" },
		func(d *Draft) { d.ScheduledAt = &sched },
	}
	for i, ch := range changes {
		d := validDraft()
		ch(&d)
		if h, _ := PayloadHash(d); bytes.Equal(h, h0) {
			t.Errorf("change %d did not change the hash", i)
		}
	}
	if len(h0) != 32 {
		t.Fatalf("hash length = %d, want 32 (SHA-256)", len(h0))
	}
}

func TestTruncateErrorKeepsUTF8Valid(t *testing.T) {
	if TruncateError("short") != "short" {
		t.Fatal("short strings must be unchanged")
	}
	s := strings.Repeat("ğ", 600) // 1200 bytes, 2 per rune
	got := TruncateError(s)
	if len(got) > MaxErrorBytes || !utf8.ValidString(got) {
		t.Fatalf("len=%d valid=%v", len(got), utf8.ValidString(got))
	}
}

func TestDeliveryReference(t *testing.T) {
	n := &Notification{CreatedAt: t0}
	if !n.DeliveryReference().Equal(t0) {
		t.Fatal("without schedule the reference is created_at")
	}
	later := t0.Add(time.Hour)
	n.ScheduledAt = &later
	if !n.DeliveryReference().Equal(later) {
		t.Fatal("a later schedule is the reference")
	}
}
