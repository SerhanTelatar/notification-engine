package redis

import (
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

func TestDecodeStatusMatchesTheContract(t *testing.T) {
	ev, err := decodeStatus(`{"notification_id":"n-1","batch_id":null,"from":null,"to":"pending","attempts":0,"at":"2026-11-18T10:00:00.123Z","correlation_id":"c-1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if ev.NotificationID != "n-1" || ev.BatchID != nil || ev.From != nil || ev.To != domain.StatusPending ||
		!ev.At.Equal(time.Date(2026, 11, 18, 10, 0, 0, 123e6, time.UTC)) || ev.CorrelationID != "c-1" {
		t.Fatalf("decoded %+v", ev)
	}
	ev, err = decodeStatus(`{"notification_id":"n-1","batch_id":"b-1","from":"queued","to":"sending","attempts":1,"at":"2026-11-18T10:00:00Z"}`)
	if err != nil || *ev.BatchID != "b-1" || *ev.From != domain.StatusQueued {
		t.Fatalf("decoded %+v, %v", ev, err)
	}
	for _, bad := range []string{`not json`, `{"to":"sent"}`, `{"notification_id":"n-1"}`} {
		if _, err := decodeStatus(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}
