package httpapi

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
)

// SuccessRoutes lists every operation in the OpenAPI spec with its lowest 1xx/2xx response code,
// for pre-initialising http_request_duration_seconds. Spec paths use chi's {param} syntax, so they
// equal the route label RequestMetrics records.
func SuccessRoutes(spec []byte) ([]metrics.HTTPRoute, error) {
	var doc struct {
		Paths map[string]map[string]struct {
			Responses map[string]any `yaml:"responses"`
		} `yaml:"paths"`
	}
	if err := yaml.Unmarshal(spec, &doc); err != nil {
		return nil, fmt.Errorf("parse openapi spec: %w", err)
	}
	var out []metrics.HTTPRoute
	for path, ops := range doc.Paths {
		for method, op := range ops {
			code := successCode(op.Responses)
			if code == "" {
				continue // not an operation (e.g. a path-level "parameters" key)
			}
			out = append(out, metrics.HTTPRoute{Route: path, Method: strings.ToUpper(method), Code: code})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Route+out[i].Method < out[j].Route+out[j].Method })
	return out, nil
}

func successCode(responses map[string]any) string {
	best := ""
	for code := range responses {
		if len(code) == 3 && (code[0] == '1' || code[0] == '2') && (best == "" || code < best) {
			best = code
		}
	}
	return best
}
