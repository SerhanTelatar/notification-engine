package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
	n, err := testutil.GatherAndCount(reg, want...)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Fatalf("gathered %d series for %d contract metrics", n, len(want))
	}
}

func TestRegistriesAreIndependent(t *testing.T) {
	// Two registries must not collide: there's no global state.
	NewAPI(NewRegistry())
	NewAPI(NewRegistry())
}
