//go:build integration

package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// One trace follows a notification across both processes and the outbox: the API request, the
// relay's publish (which may happen much later), the consumer's delivery and the provider call.
func TestOneTraceFromRequestToProvider(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tracing.NewWithProvider(tp)

	cfg := defaultPipeline(mock.SendURL())
	cfg.tracing = tr
	startPipeline(t, cfg)
	api := newAPIWith(t, quietLog, httpapi.WithTracing(tr))

	res := postJSON(t, api.URL+"/api/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"traced"}`, nil)
	if res.code != http.StatusAccepted {
		t.Fatalf("create: %d %v", res.code, res.body)
	}
	id, _ := res.body["id"].(string)
	waitFor(t, 10*time.Second, "sent", func() bool { st, _ := statusOf(t, id); return st == "sent" })

	byName := map[string]sdktrace.ReadOnlySpan{}
	waitFor(t, 5*time.Second, "all spans exported", func() bool {
		for _, s := range exporter.GetSpans().Snapshots() {
			byName[s.Name()] = s
		}
		_, a := byName["POST /api/v1/notifications"]
		_, b := byName["publish notifications.sms"]
		_, c := byName["deliver sms"]
		_, d := byName["provider send"]
		return a && b && c && d
	})
	root := byName["POST /api/v1/notifications"].SpanContext().TraceID()
	for _, name := range []string{"publish notifications.sms", "deliver sms", "provider send"} {
		if got := byName[name].SpanContext().TraceID(); got != root {
			t.Errorf("%s is in trace %s, want the request's trace %s", name, got, root)
		}
	}
	if p := byName["deliver sms"].Parent().SpanID(); p != byName["publish notifications.sms"].SpanContext().SpanID() {
		t.Errorf("deliver's parent is %s, want the publish span", p)
	}

	// The outbox row carries the request's trace context, and the provider receives it too.
	var stored string
	if err := env.Pool.QueryRow(context.Background(), `SELECT coalesce(traceparent, '') FROM notifications WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stored, root.String()) {
		t.Fatalf("stored traceparent %q, want trace %s", stored, root)
	}
	got := mock.Received()
	if len(got) != 1 || !strings.Contains(got[0].TraceParent, root.String()) {
		t.Fatalf("provider traceparent %+v, want trace %s", got, root)
	}
}

// With tracing off (no OTEL endpoint) nothing is stored or sent, and delivery is unaffected.
func TestTracingOffStoresNothing(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	startPipeline(t, defaultPipeline(mock.SendURL()))
	api := newAPI(t)
	res := postJSON(t, api.URL+"/api/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"untraced"}`, nil)
	id, _ := res.body["id"].(string)
	waitFor(t, 10*time.Second, "sent", func() bool { st, _ := statusOf(t, id); return st == "sent" })
	var stored *string
	if err := env.Pool.QueryRow(context.Background(), `SELECT traceparent FROM notifications WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != nil || mock.Received()[0].TraceParent != "" {
		t.Fatalf("tracing off but traceparent stored %v / sent %q", stored, mock.Received()[0].TraceParent)
	}
}
