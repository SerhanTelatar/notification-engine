// Package deploy holds the observability-profile configuration (Prometheus, Grafana). Its test
// keeps the dashboard in step with contracts/observability.md.
package deploy

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// contractMetrics returns every metric name in contracts/observability.md.
func contractMetrics(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile("../specs/001-notification-engine/contracts/observability.md")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, m := range regexp.MustCompile("(?m)^\\| `([a-z0-9_]+)` \\|").FindAllStringSubmatch(string(b), -1) {
		out[m[1]] = true
	}
	if len(out) < 17 {
		t.Fatalf("parsed %d metrics from the contract", len(out))
	}
	return out
}

type panel struct {
	Title   string `json:"title"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
	Datasource struct {
		UID string `json:"uid"`
	} `json:"datasource"`
}

// Every dashboard query uses only contract metrics (histograms via their _bucket series), and the
// panels the plan names are present.
func TestDashboardUsesContractMetrics(t *testing.T) {
	metrics := contractMetrics(t)
	b, err := os.ReadFile("grafana/dashboards/notifications.json")
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		UID    string  `json:"uid"`
		Panels []panel `json:"panels"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	if d.UID == "" || len(d.Panels) == 0 {
		t.Fatal("dashboard needs a uid and panels")
	}
	name := regexp.MustCompile(`\b([a-z][a-z0-9_]*_(?:total|seconds|bucket|count|sum|state|backlog|messages|connections))\b`)
	titles := map[string]bool{}
	for _, p := range d.Panels {
		titles[p.Title] = true
		if p.Datasource.UID != "prometheus" {
			t.Errorf("panel %q must use the provisioned prometheus datasource", p.Title)
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q has no query", p.Title)
		}
		for _, tg := range p.Targets {
			for _, m := range name.FindAllString(tg.Expr, -1) {
				base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(m, "_bucket"), "_count"), "_sum")
				if !metrics[m] && !metrics[base] {
					t.Errorf("panel %q queries %s, which isn't in contracts/observability.md", p.Title, m)
				}
			}
		}
	}
	for _, want := range []string{"Backlog by channel and priority", "Delivery rate by outcome",
		"End-to-end latency p95", "Provider latency p95", "Circuit breaker state"} {
		if !titles[want] {
			t.Errorf("dashboard lacks the %q panel", want)
		}
	}
}
