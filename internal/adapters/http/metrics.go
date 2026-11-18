package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
)

// RouterOption configures NewRouter.
type RouterOption func(*routerConfig)

type routerConfig struct {
	metrics *metrics.API
	tracing *tracing.Tracing
}

// WithMetrics records http_request_duration_seconds for every request.
func WithMetrics(m *metrics.API) RouterOption {
	return func(c *routerConfig) { c.metrics = m }
}

// RequestMetrics observes request latency by chi route pattern (never the raw path, so ids
// don't explode the label cardinality), method and status code.
func RequestMetrics(m *metrics.API) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			m.HTTPRequestDuration.WithLabelValues(RoutePattern(r), r.Method, strconv.Itoa(rw.status)).
				Observe(time.Since(start).Seconds())
		})
	}
}

// countCreated counts an accepted notification; idempotent replays aren't new notifications.
func (s *Server) countCreated(n *domain.Notification) {
	if s.metrics == nil || n == nil {
		return
	}
	s.metrics.NotificationsCreated.WithLabelValues(string(n.Channel), n.Priority.String()).Inc()
}

// countRejected counts a rejected create by reason.
func (s *Server) countRejected(err error) {
	if s.metrics == nil {
		return
	}
	switch {
	case errors.Is(err, domain.ErrIdempotencyConflict):
		s.metrics.NotificationsRejected.WithLabelValues("idempotency_conflict").Inc()
	case errors.Is(err, domain.ErrValidation):
		s.metrics.NotificationsRejected.WithLabelValues("validation_failed").Inc()
	}
}

// countBatch counts each item of a batch: created, or rejected by reason.
func (s *Server) countBatch(items []app.BatchItemResult) {
	if s.metrics == nil {
		return
	}
	for _, it := range items {
		switch {
		case it.Accepted && !it.Replayed:
			s.countCreated(it.Notification)
		case !it.Accepted:
			reason := "validation_failed"
			for _, fe := range it.Errors {
				if fe.Field == "idempotency_key" && fe.Message == app.KeyConflictMessage {
					reason = "idempotency_conflict"
				}
			}
			s.metrics.NotificationsRejected.WithLabelValues(reason).Inc()
		}
	}
}
