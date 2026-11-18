package metrics

import (
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Recorder implements ports.MetricsRecorder on the worker collectors, so use cases stay free of
// Prometheus (Constitution II).
type Recorder struct{ m *Worker }

var _ ports.MetricsRecorder = (*Recorder)(nil)

// NewRecorder records into m.
func NewRecorder(m *Worker) *Recorder { return &Recorder{m: m} }

// Delivered counts a provider call outcome. Latency is observed by the provider client itself
// (provider_request_duration_seconds), so it isn't recorded twice here.
func (r *Recorder) Delivered(ch domain.Channel, o domain.Outcome, _ time.Duration) {
	r.m.Delivered.WithLabelValues(string(ch), string(o)).Inc()
}

func (r *Recorder) Retried(ch domain.Channel) { r.m.Retries.WithLabelValues(string(ch)).Inc() }

func (r *Recorder) DeadLettered(ch domain.Channel, reason string) {
	r.m.DeadLettered.WithLabelValues(string(ch), reason).Inc()
}

func (r *Recorder) EndToEnd(ch domain.Channel, p domain.Priority, d time.Duration) {
	r.m.E2ELatency.WithLabelValues(string(ch), p.String()).Observe(d.Seconds())
}

func (r *Recorder) LimiterWait(ch domain.Channel, d time.Duration) {
	r.m.LimiterWait.WithLabelValues(string(ch)).Observe(d.Seconds())
}

func (r *Recorder) LimiterTimeout(ch domain.Channel) {
	r.m.LimiterTimeouts.WithLabelValues(string(ch)).Inc()
}

func (r *Recorder) RelayPublished(ch domain.Channel, n int) {
	r.m.RelayPublished.WithLabelValues(string(ch)).Add(float64(n))
}

func (r *Recorder) RelaySkipped(ch domain.Channel) { r.m.RelaySkips.WithLabelValues(string(ch)).Inc() }

func (r *Recorder) Reaped(kind string, n int) {
	r.m.ReaperRecovered.WithLabelValues(kind).Add(float64(n))
}

// Backlog sets notification_backlog; the sampler passes explicit zeros for drained combinations.
func (r *Recorder) Backlog(counts []ports.BacklogCount) {
	for _, c := range counts {
		r.m.Backlog.WithLabelValues(string(c.Channel), c.Priority.String(), string(c.Status)).Set(float64(c.Count))
	}
}

func (r *Recorder) QueueDepth(queue string, n int) {
	r.m.RabbitQueue.WithLabelValues(queue).Set(float64(n))
}
