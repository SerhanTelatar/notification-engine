package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
)

// Request latency is labelled by route pattern, never the raw path with its id.
func TestRequestMetricsUseRoutePattern(t *testing.T) {
	reg := metrics.NewRegistry()
	m := metrics.NewAPI(reg)
	h := NewRouter(quiet, NewServer(Deps{Log: quiet, Health: health.New(0)}), WithMetrics(m))
	id := "0190f4a0-0000-7000-8000-000000000001"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+id, nil))

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "http_request_duration_seconds" {
			continue
		}
		if len(f.GetMetric()) != 1 {
			t.Fatalf("series = %d, want 1", len(f.GetMetric()))
		}
		labels := map[string]string{}
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		want := map[string]string{"route": "/api/v1/notifications/{id}", "method": "GET", "code": "501"}
		for k, v := range want {
			if labels[k] != v {
				t.Fatalf("labels = %v, want %v", labels, want)
			}
		}
		if f.GetMetric()[0].GetHistogram().GetSampleCount() != 1 {
			t.Fatal("want one observation")
		}
		return
	}
	t.Fatal("http_request_duration_seconds not gathered")
}

func TestCreatedAndRejectedCounters(t *testing.T) {
	m := metrics.NewAPI(metrics.NewRegistry())
	s := NewServer(Deps{Log: quiet, Metrics: m})
	n := &domain.Notification{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}

	s.countCreated(n)
	s.countRejected(domain.NewFieldError("to", "is required"))
	s.countRejected(fmt.Errorf("wrapped: %w", domain.ErrIdempotencyConflict))
	s.countBatch([]app.BatchItemResult{
		{Accepted: true, Notification: n},
		{Accepted: true, Replayed: true, Notification: n}, // not a new notification
		{Errors: []domain.FieldError{{Field: "to", Message: "is required"}}},
		{Errors: []domain.FieldError{{Field: "idempotency_key", Message: app.KeyConflictMessage}}},
	})

	if v := testutil.ToFloat64(m.NotificationsCreated.WithLabelValues("sms", "high")); v != 2 {
		t.Fatalf("created = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.NotificationsRejected.WithLabelValues("validation_failed")); v != 2 {
		t.Fatalf("validation_failed = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.NotificationsRejected.WithLabelValues("idempotency_conflict")); v != 2 {
		t.Fatalf("idempotency_conflict = %v, want 2", v)
	}

	// Without metrics the counters are no-ops.
	NewServer(Deps{Log: quiet}).countBatch([]app.BatchItemResult{{Accepted: true, Notification: n}})
}
