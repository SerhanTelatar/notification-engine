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
	Title       string `json:"title"`
	FieldConfig struct {
		Defaults struct {
			NoValue string `json:"noValue"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
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
	window := regexp.MustCompile(`\[[^\]]+\]`)
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
			// A percentile over an idle window is NaN, which Grafana draws as nothing at all; the
			// query must drop it so the panel shows its "no traffic" text instead.
			if strings.Contains(tg.Expr, "histogram_quantile(") {
				if !strings.HasSuffix(strings.TrimSpace(tg.Expr), ">= 0") {
					t.Errorf("panel %q: wrap histogram_quantile in (…) >= 0 so idle windows aren't blank", p.Title)
				}
				if p.FieldConfig.Defaults.NoValue == "" {
					t.Errorf("panel %q: set fieldConfig.defaults.noValue for idle windows", p.Title)
				}
			}
			// Rates use Grafana's $__rate_interval (≥ 4 scrapes, ≥ step + scrape), not a fixed
			// 1m: a fixed minute smears a 3-second burst into a low, minute-long hump.
			if !strings.Contains(tg.Expr, "histogram_quantile(") {
				for _, w := range window.FindAllString(tg.Expr, -1) {
					if w != "[$__rate_interval]" {
						t.Errorf("panel %q uses the range %s; use [$__rate_interval]", p.Title, w)
					}
				}
			}
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

// $__rate_interval needs the real scrape interval (5 s); Grafana otherwise assumes 15 s.
func TestDatasourceDeclaresScrapeInterval(t *testing.T) {
	b, err := os.ReadFile("grafana/provisioning/datasources/prometheus.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^\s+timeInterval:\s*"?5s"?\s*$`).Match(b) {
		t.Fatal(`the prometheus datasource must set jsonData.timeInterval: "5s" (the scrape interval)`)
	}
}
