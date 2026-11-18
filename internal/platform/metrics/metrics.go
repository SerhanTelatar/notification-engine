// Package metrics defines every Prometheus collector in contracts/observability.md on an injected
// registry. Nothing is registered globally (Constitution II), and every latency is a histogram
// (Constitution IV).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// Label names shared across collectors.
const (
	LChannel  = "channel"
	LPriority = "priority"
	LStatus   = "status"
	LOutcome  = "outcome"
	LReason   = "reason"
	LQueue    = "queue"
	LKind     = "kind"
	LRoute    = "route"
	LMethod   = "method"
	LCode     = "code"
)

// Histogram buckets (contracts/observability.md).
var (
	ProviderBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	E2EBuckets      = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
	WaitBuckets     = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
)

// NewRegistry returns a registry with the Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// API holds the collectors emitted by cmd/api.
type API struct {
	HTTPRequestDuration   *prometheus.HistogramVec
	NotificationsCreated  *prometheus.CounterVec
	NotificationsRejected *prometheus.CounterVec
	WSConnections         prometheus.Gauge
}

// NewAPI creates and registers the API collectors on reg.
func NewAPI(reg prometheus.Registerer) *API {
	m := &API{
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by chi route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{LRoute, LMethod, LCode}),
		NotificationsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted (idempotent replays not counted).",
		}, []string{LChannel, LPriority}),
		NotificationsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_rejected_total",
			Help: "Notifications rejected by validation.",
		}, []string{LReason}),
		WSConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ws_connections",
			Help: "Open WebSocket subscriptions.",
		}),
	}
	reg.MustRegister(m.HTTPRequestDuration, m.NotificationsCreated, m.NotificationsRejected, m.WSConnections)
	for _, ch := range domain.Channels {
		for _, p := range priorities {
			m.NotificationsCreated.WithLabelValues(string(ch), p)
		}
	}
	for _, reason := range []string{"validation_failed", "idempotency_conflict"} {
		m.NotificationsRejected.WithLabelValues(reason)
	}
	return m
}

// HTTPRoute is one (route pattern, method, status code) label set of http_request_duration_seconds.
type HTTPRoute struct{ Route, Method, Code string }

// InitHTTP creates the latency series for routes at zero, so the first requests after startup
// show up in increase()/rate() instead of being absorbed into the series' first scraped value.
// Pass each operation's success code; other codes still appear on first use.
func (m *API) InitHTTP(routes []HTTPRoute) {
	for _, r := range routes {
		m.HTTPRequestDuration.WithLabelValues(r.Route, r.Method, r.Code)
	}
}

// Label values known up front. Series are pre-initialised so every contract metric appears on
// /metrics from startup (at zero), not only after its first event, which dashboards and alerts
// need.
var (
	priorities      = []string{"high", "normal", "low"}
	outcomes        = []string{"success", "transient", "permanent", "deferred", "abandoned"}
	deadReasons     = []string{"exhausted", "permanent", "provider_unavailable"}
	backlogStatuses = []string{"pending", "scheduled", "queued", "sending", "retrying"}
	queues          = []string{"notifications.sms", "notifications.email", "notifications.push", "notifications.dlq"}
)

// Worker holds the collectors emitted by cmd/worker.
type Worker struct {
	Delivered        *prometheus.CounterVec
	Retries          *prometheus.CounterVec
	DeadLettered     *prometheus.CounterVec
	Backlog          *prometheus.GaugeVec
	RabbitQueue      *prometheus.GaugeVec
	ProviderDuration *prometheus.HistogramVec
	E2ELatency       *prometheus.HistogramVec
	LimiterWait      *prometheus.HistogramVec
	LimiterTimeouts  *prometheus.CounterVec
	BreakerState     *prometheus.GaugeVec
	RelayPublished   *prometheus.CounterVec
	RelaySkips       *prometheus.CounterVec
	ReaperRecovered  *prometheus.CounterVec
}

// NewWorker creates and registers the worker collectors on reg.
func NewWorker(reg prometheus.Registerer) *Worker {
	m := &Worker{
		Delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Provider call outcomes.",
		}, []string{LChannel, LOutcome}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_retries_total",
			Help: "Transitions to retrying.",
		}, []string{LChannel}),
		DeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_dead_lettered_total",
			Help: "Transitions to failed; reason is exhausted, permanent or provider_unavailable.",
		}, []string{LChannel, LReason}),
		Backlog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_backlog",
			Help: "Non-terminal notifications by channel, priority and status (sampled from the DB).",
		}, []string{LChannel, LPriority, LStatus}),
		RabbitQueue: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "rabbitmq_queue_messages",
			Help: "Broker-side ready messages per queue.",
		}, []string{LQueue}),
		ProviderDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_request_duration_seconds",
			Help:    "Provider call latency.",
			Buckets: ProviderBuckets,
		}, []string{LChannel, LOutcome}),
		E2ELatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_e2e_latency_seconds",
			Help:    "sent_at minus max(created_at, scheduled_at).",
			Buckets: E2EBuckets,
		}, []string{LChannel, LPriority}),
		LimiterWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rate_limiter_wait_seconds",
			Help:    "Time spent waiting for a rate-limit permit.",
			Buckets: WaitBuckets,
		}, []string{LChannel}),
		LimiterTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limiter_timeouts_total",
			Help: "Messages requeued because no permit was obtained within LIMITER_MAX_WAIT.",
		}, []string{LChannel}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "0 closed, 1 half-open, 2 open.",
		}, []string{LChannel}),
		RelayPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_published_total",
			Help: "Messages published by the outbox relay.",
		}, []string{LChannel}),
		RelaySkips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_backpressure_skips_total",
			Help: "Relay ticks that skipped a channel at RELAY_MAX_INFLIGHT_PER_CHANNEL.",
		}, []string{LChannel}),
		ReaperRecovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_recovered_total",
			Help: "Crash-recovery actions by kind (sending, queued).",
		}, []string{LKind}),
	}
	reg.MustRegister(m.Delivered, m.Retries, m.DeadLettered, m.Backlog, m.RabbitQueue,
		m.ProviderDuration, m.E2ELatency, m.LimiterWait, m.LimiterTimeouts, m.BreakerState,
		m.RelayPublished, m.RelaySkips, m.ReaperRecovered)
	for _, ch := range domain.Channels {
		c := string(ch)
		for _, o := range outcomes {
			m.Delivered.WithLabelValues(c, o)
			m.ProviderDuration.WithLabelValues(c, o)
		}
		for _, r := range deadReasons {
			m.DeadLettered.WithLabelValues(c, r)
		}
		for _, p := range priorities {
			m.E2ELatency.WithLabelValues(c, p)
			for _, s := range backlogStatuses {
				m.Backlog.WithLabelValues(c, p, s)
			}
		}
		m.Retries.WithLabelValues(c)
		m.LimiterWait.WithLabelValues(c)
		m.LimiterTimeouts.WithLabelValues(c)
		m.BreakerState.WithLabelValues(c)
		m.RelayPublished.WithLabelValues(c)
		m.RelaySkips.WithLabelValues(c)
	}
	for _, q := range queues {
		m.RabbitQueue.WithLabelValues(q)
	}
	for _, k := range []string{"sending", "queued"} {
		m.ReaperRecovered.WithLabelValues(k)
	}
	return m
}
