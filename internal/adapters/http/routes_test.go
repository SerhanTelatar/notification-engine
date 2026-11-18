package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/SerhanTelatar/notification-engine/api"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
)

// The success code of every operation comes from the contract.
func TestSuccessRoutesComeFromTheContract(t *testing.T) {
	routes, err := SuccessRoutes(api.Spec)
	if err != nil {
		t.Fatal(err)
	}
	have := map[metrics.HTTPRoute]bool{}
	for _, r := range routes {
		have[r] = true
	}
	for _, want := range []metrics.HTTPRoute{
		{Route: "/api/v1/notifications", Method: http.MethodPost, Code: "202"},
		{Route: "/api/v1/notifications/batch", Method: http.MethodPost, Code: "202"},
		{Route: "/api/v1/notifications/{id}", Method: http.MethodGet, Code: "200"},
		{Route: "/api/v1/templates", Method: http.MethodPost, Code: "201"},
		{Route: "/api/v1/metrics/summary", Method: http.MethodGet, Code: "200"},
	} {
		if !have[want] {
			t.Errorf("missing %+v in %v", want, routes)
		}
	}
	if _, err := SuccessRoutes([]byte("not: [yaml")); err == nil {
		t.Fatal("a malformed spec must be an error")
	}
}

// Every route the router serves is in the contract, so pre-initialising from the contract covers
// them all (the label is the chi pattern, which matches the spec path).
func TestEveryRouterRouteIsInTheContract(t *testing.T) {
	routes, err := SuccessRoutes(api.Spec)
	if err != nil {
		t.Fatal(err)
	}
	inSpec := map[string]bool{}
	for _, r := range routes {
		inSpec[r.Method+" "+r.Route] = true
	}
	r := NewRouter(quiet, NewServer(Deps{Log: quiet, Health: health.New(0)}))
	MountOps(r, metrics.NewRegistry(), api.Spec)
	err = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		route = strings.TrimSuffix(route, "/")
		if !inSpec[method+" "+route] {
			t.Errorf("router serves %s %s, which isn't in api/openapi.yaml", method, route)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
