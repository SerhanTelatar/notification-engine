package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
)

func TestCorrelationID(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		wantSame bool
	}{
		{"echoes a valid header", "req-123", true},
		{"generates when absent", "", false},
		{"replaces an over-long id", strings.Repeat("x", 129), false},
		{"replaces non-printable", "bad\nid", false},
		{"replaces non-ascii", "kimlik-ğ", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var seen string
			h := CorrelationID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = logger.CorrelationID(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set(HeaderCorrelationID, tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			echoed := rec.Header().Get(HeaderCorrelationID)
			if echoed == "" || echoed != seen {
				t.Fatalf("echoed %q, ctx %q", echoed, seen)
			}
			if (echoed == tc.header) != tc.wantSame {
				t.Fatalf("echoed %q for header %q", echoed, tc.header)
			}
		})
	}
}

func TestRecoverHidesInternals(t *testing.T) {
	var logs bytes.Buffer
	log := logger.NewWithWriter(&logs, "api", "info")
	h := Recover(log)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("database password is hunter2")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatal("panic value leaked to the client")
	}
	if !strings.Contains(logs.String(), "hunter2") {
		t.Fatal("panic must be logged for operators")
	}
}

func TestAccessLogUsesRoutePatternAndCorrelation(t *testing.T) {
	var logs bytes.Buffer
	log := logger.NewWithWriter(&logs, "api", "info")
	r := chi.NewRouter()
	r.Use(CorrelationID, AccessLog(log))
	r.Get("/api/v1/notifications/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("abc"))
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/0193-secret-id", nil)
	req.Header.Set(HeaderCorrelationID, "c-42")
	r.ServeHTTP(httptest.NewRecorder(), req)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &line); err != nil {
		t.Fatalf("log line not JSON: %q", logs.String())
	}
	if line["route"] != "/api/v1/notifications/{id}" || line["status"] != float64(418) ||
		line["bytes"] != float64(3) || line["correlation_id"] != "c-42" {
		t.Fatalf("unexpected access log: %v", line)
	}
	if strings.Contains(logs.String(), "0193-secret-id") {
		t.Fatal("raw path must not be logged")
	}
}

func TestTimeoutSetsDeadline(t *testing.T) {
	var ok bool
	h := Timeout(time.Second)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		dl, has := r.Context().Deadline()
		ok = has && time.Until(dl) <= time.Second
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !ok {
		t.Fatal("request context has no deadline")
	}
}

func TestRouterAppliesMiddleware(t *testing.T) {
	h := NewRouter(quiet, NewServer(quiet, health.New(0)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/templates", nil))
	if rec.Header().Get(HeaderCorrelationID) == "" {
		t.Fatal("router must set X-Correlation-ID on every response")
	}
}
