// Package tracing sets up OpenTelemetry tracing (bonus). Nothing is registered globally
// (Constitution II): adapters receive a *Tracing and use its provider and propagator explicitly.
// With no OTEL_EXPORTER_OTLP_ENDPOINT, tracing is a no-op and costs nothing.
package tracing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// InstrumentationName names the tracer used by this codebase's own spans.
const InstrumentationName = "github.com/SerhanTelatar/notification-engine"

// HeaderTraceParent is the W3C trace context header, on HTTP and on AMQP messages.
const HeaderTraceParent = "traceparent"

// w3c is the only propagation format used: W3C trace context.
var w3c = propagation.TraceContext{}

// Tracing holds a tracer provider and the propagator to use with it.
type Tracing struct {
	provider trace.TracerProvider
	enabled  bool
	shutdown func(context.Context) error
}

// New exports spans over OTLP/HTTP to endpoint (e.g. http://jaeger:4318), tagged with service.
// An empty endpoint returns Disabled().
func New(ctx context.Context, endpoint, service string) (*Tracing, error) {
	if strings.TrimSpace(endpoint) == "" {
		return Disabled(), nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimRight(endpoint, "/")+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	return &Tracing{provider: tp, enabled: true, shutdown: tp.Shutdown}, nil
}

// NewWithProvider wraps an existing provider (tests use an in-memory exporter).
func NewWithProvider(tp trace.TracerProvider) *Tracing {
	return &Tracing{provider: tp, enabled: true, shutdown: func(context.Context) error { return nil }}
}

// Disabled is tracing switched off: spans are no-ops and no trace context is produced.
func Disabled() *Tracing {
	return &Tracing{provider: noop.NewTracerProvider(), shutdown: func(context.Context) error { return nil }}
}

// Enabled reports whether spans are recorded and exported.
func (t *Tracing) Enabled() bool { return t != nil && t.enabled }

// Provider is the tracer provider to pass to instrumentation (otelhttp).
func (t *Tracing) Provider() trace.TracerProvider {
	if t == nil {
		return noop.NewTracerProvider()
	}
	return t.provider
}

// Propagator is the W3C trace context propagator.
func (t *Tracing) Propagator() propagation.TextMapPropagator { return w3c }

// Tracer is this codebase's tracer.
func (t *Tracing) Tracer() trace.Tracer { return t.Provider().Tracer(InstrumentationName) }

// Shutdown flushes buffered spans.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	return t.shutdown(ctx)
}

// TraceParent returns the W3C traceparent of the span in ctx, or "" when there is none (tracing
// off). It's what the outbox row and AMQP messages carry between processes.
func TraceParent(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	w3c.Inject(ctx, c)
	return c[HeaderTraceParent]
}

// ContextWithTraceParent makes the span described by traceparent the remote parent in ctx. An
// empty or malformed value leaves ctx unchanged.
func ContextWithTraceParent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return w3c.Extract(ctx, propagation.MapCarrier{HeaderTraceParent: traceparent})
}
