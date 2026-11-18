package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestUnbuiltOperationsReturn501(t *testing.T) {
	h := NewRouter(quiet, NewServer(Deps{Log: quiet, Health: health.New(0)}))
	tests := []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"hi"}`},
		{http.MethodGet, "/api/v1/notifications", ""},
		{http.MethodGet, "/api/v1/notifications/0193a3b0-0000-7000-8000-000000000001", ""},
		{http.MethodPost, "/api/v1/batches/0193a3b0-0000-7000-8000-000000000001/cancel", ""},
		{http.MethodGet, "/api/v1/templates", ""},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501; body %s", rec.Code, rec.Body)
			}
			var body errorBody
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil || body.Error.Code != "internal" {
				t.Fatalf("error shape wrong: %v %+v", err, body)
			}
		})
	}
}

func TestBadPathParamIs400(t *testing.T) {
	h := NewRouter(quiet, NewServer(Deps{Log: quiet, Health: health.New(0)}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/notifications/not-a-uuid", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHTTPServerLimits(t *testing.T) {
	srv := NewHTTPServer(":0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout != ReadHeaderTimeout || srv.WriteTimeout != WriteTimeout {
		t.Fatal("timeouts not applied")
	}
}
