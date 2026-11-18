//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

func execSQL(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := env.Pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func addAttempt(t *testing.T, id string, no, latencyMs int, outcome string) {
	t.Helper()
	execSQL(t, `INSERT INTO notification_attempts (notification_id, attempt_no, started_at, finished_at, latency_ms, outcome)
		VALUES ($1, $2, now() - interval '20 seconds', now() - interval '20 seconds', $3, $4)`, id, no, latencyMs, outcome)
}

// GET /api/v1/metrics/summary computes the last 5 minutes from the database (R16).
func TestMetricsSummary(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	sms := func() string { return mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil)).ID }

	// Two sms sent in the window: e2e 1000 ms and 3000 ms, provider 100 ms and 300 ms.
	for _, c := range []struct{ e2eSec, latency int }{{1, 100}, {3, 300}} {
		id := sms()
		execSQL(t, `UPDATE notifications SET status = 'sent', attempts = 1, created_at = now() - interval '60 seconds',
			sent_at = now() - interval '60 seconds' + make_interval(secs => $2) WHERE id = $1`, id, c.e2eSec)
		addAttempt(t, id, 1, c.latency, "success")
	}
	// One sent 10 minutes ago: outside the window.
	old := sms()
	execSQL(t, `UPDATE notifications SET status = 'sent', attempts = 1, created_at = now() - interval '11 minutes',
		sent_at = now() - interval '10 minutes' WHERE id = $1`, old)
	// One failed in the window after 5 transient attempts: 4 of them led to a retry.
	failed := sms()
	execSQL(t, `UPDATE notifications SET status = 'failed', attempts = 5, failed_at = now() - interval '10 seconds',
		last_error = 'provider returned HTTP 503' WHERE id = $1`, failed)
	for no := 1; no <= 5; no++ {
		addAttempt(t, failed, no, 50, "transient")
	}
	// Backlog: one email pending.
	mustCreate(t, repo, newNotification(t, domain.ChannelEmail, nil))

	code, body := getJSON(t, newAPI(t).URL+"/api/v1/metrics/summary")
	if code != http.StatusOK || body["window_seconds"] != float64(300) {
		t.Fatalf("summary: %d %v", code, body)
	}
	backlog, _ := body["backlog"].([]any)
	if len(backlog) != 1 {
		t.Fatalf("backlog %v, want the single pending email", backlog)
	}
	if b := backlog[0].(map[string]any); b["channel"] != "email" || b["status"] != "pending" || b["priority"] != "normal" || b["count"] != float64(1) {
		t.Fatalf("backlog row %v", b)
	}

	channels, _ := body["channels"].([]any)
	if len(channels) != 3 {
		t.Fatalf("channels %v, want all three", channels)
	}
	got := channels[0].(map[string]any)
	if got["channel"] != "sms" || got["sent"] != float64(2) || got["failed"] != float64(1) || got["retried"] != float64(4) {
		t.Fatalf("sms counts %v", got)
	}
	if rate := got["success_rate"].(float64); rate < 0.285 || rate > 0.286 { // 2 successes / 7 attempts
		t.Fatalf("success_rate %v, want 2/7", rate)
	}
	e2e := got["e2e_latency_ms"].(map[string]any)
	if e2e["p50"] != float64(2000) {
		t.Fatalf("e2e %v, want p50 2000 ms", e2e)
	}
	provider := got["provider_latency_ms"].(map[string]any)
	if provider["p50"] != float64(50) || provider["p99"].(float64) < 280 {
		t.Fatalf("provider latency %v", provider)
	}
	if email := channels[1].(map[string]any); email["channel"] != "email" || email["sent"] != float64(0) || email["e2e_latency_ms"] != nil {
		t.Fatalf("quiet email channel %v", email)
	}
}
