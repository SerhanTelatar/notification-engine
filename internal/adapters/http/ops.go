package httpapi

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
)

// GetLiveness reports that the process is up; it never touches dependencies.
func (s *Server) GetLiveness(context.Context, gen.GetLivenessRequestObject) (gen.GetLivenessResponseObject, error) {
	return gen.GetLiveness200JSONResponse(toHealth(health.Live())), nil
}

// GetReadiness checks the real dependencies: 503 if a required one is down, 200 (possibly
// degraded) otherwise.
func (s *Server) GetReadiness(ctx context.Context, _ gen.GetReadinessRequestObject) (gen.GetReadinessResponseObject, error) {
	res := s.health.Ready(ctx)
	if res.HTTPStatus() == http.StatusServiceUnavailable {
		return gen.GetReadiness503JSONResponse(toHealth(res)), nil
	}
	return gen.GetReadiness200JSONResponse(toHealth(res)), nil
}

func toHealth(r health.Result) gen.Health {
	h := gen.Health{Status: gen.HealthStatus(r.Status)}
	if len(r.Checks) > 0 {
		m := make(map[string]gen.HealthChecks, len(r.Checks))
		for k, v := range r.Checks {
			m[k] = gen.HealthChecks(v)
		}
		h.Checks = &m
	}
	return h
}

// MountOps adds the hand-mounted, non-generated routes (tag ops-raw in api/openapi.yaml):
// Prometheus /metrics, Swagger UI at /swagger and the raw contract at /openapi.yaml.
func MountOps(r chi.Router, g prometheus.Gatherer, spec []byte) {
	r.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(spec)
	})
	r.Get("/swagger", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerHTML))
	})
}

// swaggerHTML renders /openapi.yaml with a pinned Swagger UI release from the jsDelivr CDN.
const swaggerHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>notification-engine API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css">
  <style>body { margin: 0; background: #fafafa; }</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui", deepLinking: true });
  </script>
</body>
</html>
`
