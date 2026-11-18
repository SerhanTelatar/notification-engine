package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Every metric named in contracts/observability.md must exist with exactly those labels.
func TestCollectorsMatchContract(t *testing.T) {
	reg := NewRegistry()
	api := NewAPI(reg)
	w := NewWorker(reg)

	// Touch one series per vector so Gather reports it.
	api.HTTPRequestDuration.WithLabelValues("/api/v1/notifications", "POST", "202").Observe(0.01)
	api.NotificationsCreated.WithLabelValues("sms", "high").Inc()
	api.NotificationsRejected.WithLabelValues("validation_failed").Inc()
	api.WSConnections.Set(1)
	w.Delivered.WithLabelValues("sms", "success").Inc()
	w.Retries.WithLabelValues("sms").Inc()
	w.DeadLettered.WithLabelValues("sms", "provider_unavailable").Inc()
	w.Backlog.WithLabelValues("sms", "high", "queued").Set(3)
	w.RabbitQueue.WithLabelValues("notifications.sms").Set(3)
	w.ProviderDuration.WithLabelValues("sms", "success").Observe(0.1)
	w.E2ELatency.WithLabelValues("sms", "high").Observe(1)
	w.LimiterWait.WithLabelValues("sms").Observe(0.01)
	w.LimiterTimeouts.WithLabelValues("sms").Inc()
	w.BreakerState.WithLabelValues("sms").Set(2)
	w.RelayPublished.WithLabelValues("sms").Inc()
	w.RelaySkips.WithLabelValues("sms").Inc()
	w.ReaperRecovered.WithLabelValues("sending").Inc()

	want := []string{
		"http_request_duration_seconds", "notifications_created_total", "notifications_rejected_total",
		"ws_connections", "notifications_delivered_total", "notification_retries_total",
		"notifications_dead_lettered_total", "notification_backlog", "rabbitmq_queue_messages",
		"provider_request_duration_seconds", "notification_e2e_latency_seconds",
		"rate_limiter_wait_seconds", "rate_limiter_timeouts_total", "circuit_breaker_state",
		"relay_published_total", "relay_backpressure_skips_total", "reaper_recovered_total",
	}
	// Series are pre-initialised, so count families, not series.
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, f := range families {
		have[f.GetName()] = true
	}
	for _, name := range want {
		if !have[name] {
			t.Errorf("contract metric %s not registered", name)
		}
	}
}

func TestRegistriesAreIndependent(t *testing.T) {
	// Two registries must not collide: there's no global state.
	NewAPI(NewRegistry())
	NewAPI(NewRegistry())
}

// Every worker metric in the contract is visible right after startup, before any event.
func TestWorkerMetricsArePreInitialised(t *testing.T) {
	reg := NewRegistry()
	NewWorker(reg)
	want := []string{
		"notifications_delivered_total", "notification_retries_total", "notifications_dead_lettered_total",
		"notification_backlog", "rabbitmq_queue_messages", "provider_request_duration_seconds",
		"notification_e2e_latency_seconds", "rate_limiter_wait_seconds", "rate_limiter_timeouts_total",
		"circuit_breaker_state", "relay_published_total", "relay_backpressure_skips_total", "reaper_recovered_total",
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, f := range families {
		have[f.GetName()] = true
	}
	for _, name := range want {
		if !have[name] {
			t.Errorf("%s missing before any event", name)
		}
	}
}

func TestRecorderWritesTheCollectors(t *testing.T) {
	reg := NewRegistry()
	w := NewWorker(reg)
	r := NewRecorder(w)
	r.DeadLettered("sms", "provider_unavailable")
	r.Backlog([]ports.BacklogCount{{Channel: "email", Priority: 3, Status: "queued", Count: 4}})
	r.QueueDepth("notifications.dlq", 2)
	r.Reaped("sending", 3)
	if v := testutil.ToFloat64(w.DeadLettered.WithLabelValues("sms", "provider_unavailable")); v != 1 {
		t.Fatalf("dead lettered = %v", v)
	}
	if v := testutil.ToFloat64(w.Backlog.WithLabelValues("email", "high", "queued")); v != 4 {
		t.Fatalf("backlog = %v", v)
	}
	if v := testutil.ToFloat64(w.RabbitQueue.WithLabelValues("notifications.dlq")); v != 2 {
		t.Fatalf("queue depth = %v", v)
	}
	if v := testutil.ToFloat64(w.ReaperRecovered.WithLabelValues("sending")); v != 3 {
		t.Fatalf("reaped = %v", v)
	}
}

// HTTP latency series exist at zero before the first request, so increase()/rate() in Grafana
// count the very first requests instead of starting from the first scraped value.
func TestInitHTTPPreCreatesRouteSeries(t *testing.T) {
	reg := NewRegistry()
	m := NewAPI(reg)
	m.InitHTTP([]HTTPRoute{{Route: "/api/v1/notifications", Method: "POST", Code: "202"}})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "http_request_duration_seconds" {
			continue
		}
		if len(f.GetMetric()) != 1 || f.GetMetric()[0].GetHistogram().GetSampleCount() != 0 {
			t.Fatalf("want one zero series, got %v", f.GetMetric())
		}
		return
	}
	t.Fatal("http_request_duration_seconds not exposed before the first request")
}
