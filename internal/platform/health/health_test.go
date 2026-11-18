package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ok(context.Context) error   { return nil }
func down(context.Context) error { return errors.New("connection refused") }
func hang(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestReady(t *testing.T) {
	tests := []struct {
		name       string
		build      func(*Checker)
		wantStatus Status
		wantCode   int
		wantChecks map[string]Status
	}{
		{
			name:       "no dependencies",
			build:      func(*Checker) {},
			wantStatus: StatusOK, wantCode: 200, wantChecks: map[string]Status{},
		},
		{
			name:       "all healthy",
			build:      func(c *Checker) { c.Require("postgres", ok).Optional("redis", ok) },
			wantStatus: StatusOK, wantCode: 200,
			wantChecks: map[string]Status{"postgres": StatusOK, "redis": StatusOK},
		},
		{
			name:       "optional down degrades but stays ready",
			build:      func(c *Checker) { c.Require("postgres", ok).Optional("redis", down) },
			wantStatus: StatusDegraded, wantCode: 200,
			wantChecks: map[string]Status{"postgres": StatusOK, "redis": StatusDegraded},
		},
		{
			name:       "required down is unavailable",
			build:      func(c *Checker) { c.Require("postgres", ok).Require("rabbitmq", down).Optional("redis", down) },
			wantStatus: StatusUnavailable, wantCode: 503,
			wantChecks: map[string]Status{"postgres": StatusOK, "rabbitmq": StatusUnavailable, "redis": StatusDegraded},
		},
		{
			name:       "a hanging check times out as a failure",
			build:      func(c *Checker) { c.Require("postgres", hang) },
			wantStatus: StatusUnavailable, wantCode: 503,
			wantChecks: map[string]Status{"postgres": StatusUnavailable},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New(50 * time.Millisecond)
			tc.build(c)
			start := time.Now()
			res := c.Ready(context.Background())
			if time.Since(start) > time.Second {
				t.Fatal("checks must be bounded by the timeout")
			}
			if res.Status != tc.wantStatus || res.HTTPStatus() != tc.wantCode {
				t.Fatalf("got %s/%d, want %s/%d", res.Status, res.HTTPStatus(), tc.wantStatus, tc.wantCode)
			}
			for k, v := range tc.wantChecks {
				if res.Checks[k] != v {
					t.Errorf("check %s = %s, want %s", k, res.Checks[k], v)
				}
			}
		})
	}
}

func TestHandlers(t *testing.T) {
	c := New(0).Require("rabbitmq", down)

	rec := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness must not depend on dependencies, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	c.ReadinessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness = %d, want 503", rec.Code)
	}
	var body Result
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != StatusUnavailable || body.Checks["rabbitmq"] != StatusUnavailable {
		t.Fatalf("unexpected body %+v", body)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatal("content type must be JSON")
	}
}
