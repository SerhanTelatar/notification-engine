package ws

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

func event(id string, batch *string, to domain.Status) ports.StatusEvent {
	from := domain.StatusQueued
	return ports.StatusEvent{NotificationID: id, BatchID: batch, From: &from, To: to, Attempts: 1,
		At: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), CorrelationID: "c-1"}
}

func recv(t *testing.T, s *Subscription) map[string]any {
	t.Helper()
	select {
	case b := <-s.Messages():
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	case <-time.After(time.Second):
		t.Fatal("no message")
		return nil
	}
}

func none(t *testing.T, s *Subscription) {
	t.Helper()
	select {
	case b := <-s.Messages():
		t.Fatalf("unexpected message %s", b)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestHubFansOutByNotificationAndBatch(t *testing.T) {
	h := NewHub(0)
	batch := "b-1"
	byID := h.Subscribe("n-1", "")
	byBatch := h.Subscribe("", batch)
	other := h.Subscribe("n-2", "")

	h.Publish(event("n-1", &batch, domain.StatusSending))
	m := recv(t, byID)
	if m["type"] != "status" || m["notification_id"] != "n-1" || m["batch_id"] != "b-1" ||
		m["from"] != "queued" || m["to"] != "sending" || m["attempts"] != float64(1) || m["at"] == nil {
		t.Fatalf("status message %v", m)
	}
	if _, ok := m["correlation_id"]; ok {
		t.Fatal("the client message carries only the contract fields")
	}
	if recv(t, byBatch)["notification_id"] != "n-1" {
		t.Fatal("batch subscriber must get its items' events")
	}
	none(t, other)

	h.Publish(event("n-3", nil, domain.StatusSent)) // nobody subscribed
	none(t, byID)
	none(t, byBatch)
}

func TestHubUnsubscribeOnDisconnect(t *testing.T) {
	h := NewHub(0)
	s := h.Subscribe("n-1", "")
	if h.Len() != 1 {
		t.Fatalf("len = %d", h.Len())
	}
	s.Close() // the client went away
	s.Close() // idempotent
	if h.Len() != 0 {
		t.Fatalf("len = %d after close", h.Len())
	}
	h.Publish(event("n-1", nil, domain.StatusSent)) // must not panic or block
	select {
	case <-s.Done():
	default:
		t.Fatal("Done must be closed")
	}
}

func TestHubDropsSlowClientWithPolicyViolation(t *testing.T) {
	h := NewHub(4)
	slow := h.Subscribe("n-1", "")
	fast := h.Subscribe("n-1", "")
	for range 4 {
		h.Publish(event("n-1", nil, domain.StatusSending))
		<-fast.Messages()
	}
	h.Publish(event("n-1", nil, domain.StatusSent)) // slow's buffer is full: drop it, don't block
	select {
	case <-slow.Done():
	case <-time.After(time.Second):
		t.Fatal("a full buffer must close the subscription")
	}
	if slow.CloseCode() != ClosePolicyViolation {
		t.Fatalf("close code %d, want 1008", slow.CloseCode())
	}
	if recv(t, fast)["to"] != "sent" {
		t.Fatal("other clients keep receiving")
	}
	if h.Len() != 1 {
		t.Fatalf("len = %d, the slow client must be removed", h.Len())
	}
}

func TestHubShutdownClosesEveryoneGoingAway(t *testing.T) {
	h := NewHub(0)
	a, b := h.Subscribe("n-1", ""), h.Subscribe("", "b-1")
	h.Shutdown()
	for _, s := range []*Subscription{a, b} {
		select {
		case <-s.Done():
		case <-time.After(time.Second):
			t.Fatal("shutdown must close every subscription")
		}
		if s.CloseCode() != CloseGoingAway {
			t.Fatalf("close code %d, want 1001", s.CloseCode())
		}
	}
	late := h.Subscribe("n-1", "") // after shutdown: closed straight away
	select {
	case <-late.Done():
	default:
		t.Fatal("subscribing after shutdown must return a closed subscription")
	}
}
