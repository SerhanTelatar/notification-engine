package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SerhanTelatar/notification-engine/api"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
)

func opsRouter(checker *health.Checker) http.Handler {
	r := NewRouter(quiet, NewServer(Deps{Log: quiet, Health: checker}))
	reg := metrics.NewRegistry()
	metrics.NewAPI(reg)
	MountOps(r, reg, api.Spec)
	return r
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthEndpoints(t *testing.T) {
	down := func(context.Context) error { return errors.New("down") }
	up := func(context.Context) error { return nil }
	tests := []struct {
		name       string
		checker    *health.Checker
		path       string
		wantCode   int
		wantStatus string
	}{
		{"liveness ignores dependencies", health.New(0).Require("postgres", down), "/healthz", 200, "ok"},
		{"ready when required deps are up", health.New(0).Require("postgres", up), "/readyz", 200, "ok"},
		{"degraded optional stays 200", health.New(0).Require("postgres", up).Optional("redis", down), "/readyz", 200, "degraded"},
		{"required down is 503", health.New(0).Require("postgres", down), "/readyz", 503, "unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := get(t, opsRouter(tc.checker), tc.path)
			var body struct {
				Status string            `json:"status"`
				Checks map[string]string `json:"checks"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tc.wantCode || body.Status != tc.wantStatus {
				t.Fatalf("%s: %d %s", tc.path, rec.Code, rec.Body)
			}
		})
	}
}

func TestRawOpsRoutes(t *testing.T) {
	h := opsRouter(health.New(0))
	if rec := get(t, h, "/metrics"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Fatalf("/metrics: %d", rec.Code)
	}
	if rec := get(t, h, "/openapi.yaml"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "openapi: 3.0.3") {
		t.Fatalf("/openapi.yaml: %d", rec.Code)
	}
	if rec := get(t, h, "/swagger"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "swagger-ui-dist@5.17.14") {
		t.Fatalf("/swagger: %d", rec.Code)
	}
}
