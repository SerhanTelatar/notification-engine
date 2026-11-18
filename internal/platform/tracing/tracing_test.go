package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestDisabledProducesNoTraceContext(t *testing.T) {
	tr, err := New(context.Background(), "", "api")
	if err != nil || tr.Enabled() {
		t.Fatalf("empty endpoint must disable tracing: %v", err)
	}
	ctx, span := tr.Tracer().Start(context.Background(), "x")
	defer span.End()
	if TraceParent(ctx) != "" {
		t.Fatal("a no-op span has no trace context to propagate")
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTraceParentRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tr := NewWithProvider(tp)
	ctx, span := tr.Tracer().Start(context.Background(), "request")
	defer span.End()

	tp1 := TraceParent(ctx)
	if len(tp1) != 55 || tp1[:3] != "00-" {
		t.Fatalf("traceparent %q is not W3C", tp1)
	}
	remote := trace.SpanContextFromContext(ContextWithTraceParent(context.Background(), tp1))
	if !remote.IsRemote() || remote.TraceID() != span.SpanContext().TraceID() || remote.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("round trip lost the span: %+v", remote)
	}
	if ContextWithTraceParent(context.Background(), "garbage") == nil ||
		trace.SpanContextFromContext(ContextWithTraceParent(context.Background(), "garbage")).IsValid() {
		t.Fatal("a malformed traceparent must be ignored")
	}
}

func TestNewWithEndpointBuildsAnExporter(t *testing.T) {
	tr, err := New(context.Background(), "http://127.0.0.1:4318/", "worker")
	if err != nil || !tr.Enabled() {
		t.Fatalf("New = %v, %v", tr, err)
	}
	_ = tr.Shutdown(context.Background()) // nothing listening; flushing nothing must not hang
}
