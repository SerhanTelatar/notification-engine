package httpapi

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
)

// WithTracing traces every API request (bonus, T117). Off when t is nil or disabled.
func WithTracing(t *tracing.Tracing) RouterOption {
	return func(c *routerConfig) { c.tracing = t }
}

// untraced paths: scrapes and probes are noise, and /ws spans would last as long as the socket.
var untraced = map[string]bool{"/metrics": true, "/healthz": true, "/readyz": true, "/ws": true}

// RequestTracing starts a server span per request (continuing an incoming traceparent) and,
// once chi has routed it, names the span "<METHOD> <route pattern>", never the raw path.
func RequestTracing(t *tracing.Tracing) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		named := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			route := RoutePattern(r)
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route),
				attribute.String("correlation_id", logger.CorrelationID(r.Context())))
		})
		return otelhttp.NewHandler(named, "http.request",
			otelhttp.WithTracerProvider(t.Provider()),
			otelhttp.WithPropagators(t.Propagator()),
			otelhttp.WithFilter(func(r *http.Request) bool { return !untraced[r.URL.Path] }),
		)
	}
}
