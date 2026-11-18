package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) Lookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoadWorkerDefaults(t *testing.T) {
	c, err := LoadWorker(env(nil))
	if err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"RateLimitPerSecond", c.RateLimitPerSecond, 100},
		{"RelayMaxInflightPerChannel", c.RelayMaxInflightPerChannel, 1000},
		{"BreakerMaxDeferAge", c.BreakerMaxDeferAge, time.Hour},
		{"LimiterMaxWait", c.LimiterMaxWait, 30 * time.Second},
		{"RelayInterval", c.RelayInterval, 200 * time.Millisecond},
		{"ProviderTimeout", c.ProviderTimeout, 5 * time.Second},
		{"ReaperQueuedTimeout", c.ReaperQueuedTimeout, 15 * time.Minute},
		{"LogLevel", c.LogLevel, "info"},
	}
	for _, tc := range checks {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

func TestLoadAPIOverrides(t *testing.T) {
	c, err := LoadAPI(env(map[string]string{"HTTP_ADDR": ":9999", "MAX_ATTEMPTS": "7", "LOG_LEVEL": "DEBUG"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9999" || c.MaxAttempts != 7 || c.LogLevel != "debug" {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadReportsEveryError(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		wantAll []string
	}{
		{
			name: "several invalid values at once",
			vars: map[string]string{
				"RATE_LIMIT_PER_SECOND": "fast",
				"RELAY_INTERVAL":        "soon",
				"LOG_LEVEL":             "loud",
				"PROVIDER_URL":          "ftp://x/y",
			},
			wantAll: []string{"RATE_LIMIT_PER_SECOND", "RELAY_INTERVAL", "LOG_LEVEL", "PROVIDER_URL"},
		},
		{
			name:    "out of range and non-positive",
			vars:    map[string]string{"RELAY_BATCH_SIZE": "0", "BREAKER_COOLDOWN": "-1s"},
			wantAll: []string{"RELAY_BATCH_SIZE", "BREAKER_COOLDOWN"},
		},
		{
			name:    "cross-field rules",
			vars:    map[string]string{"BACKOFF_BASE": "10m", "REAPER_SENDING_TIMEOUT": "1s"},
			wantAll: []string{"BACKOFF_MAX must be >= BACKOFF_BASE", "REAPER_SENDING_TIMEOUT"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadWorker(env(tc.vars))
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range tc.wantAll {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

func TestURLErrorsDoNotLeakCredentials(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{"DATABASE_URL": "mysql://user:s3cret@db/x"}))
	if err == nil {
		t.Fatal("expected scheme error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("error leaks password: %v", err)
	}
}
