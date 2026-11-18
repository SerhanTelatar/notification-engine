package domain

import (
	"errors"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	netErr := errors.New("dial tcp: connection refused")
	tests := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{"202 accepted", 202, nil, OutcomeSuccess},
		{"200 ok (webhook.site default)", 200, nil, OutcomeSuccess},
		{"204 no content", 204, nil, OutcomeSuccess},
		{"429 rate limited", 429, nil, OutcomeTransient},
		{"500", 500, nil, OutcomeTransient},
		{"503", 503, nil, OutcomeTransient},
		{"599", 599, nil, OutcomeTransient},
		{"network error", 0, netErr, OutcomeTransient},
		{"timeout", 0, errors.New("context deadline exceeded"), OutcomeTransient},
		{"error wins over a status", 200, netErr, OutcomeTransient},
		{"400 bad request", 400, nil, OutcomePermanent},
		{"401", 401, nil, OutcomePermanent},
		{"404", 404, nil, OutcomePermanent},
		{"408 is permanent per FR-020", 408, nil, OutcomePermanent},
		{"422", 422, nil, OutcomePermanent},
		{"1xx unexpected", 101, nil, OutcomePermanent},
		{"3xx redirect not followed", 302, nil, OutcomePermanent},
		{"no status no error", 0, nil, OutcomePermanent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.status, tc.err); got != tc.want {
				t.Fatalf("Classify(%d, %v) = %s, want %s", tc.status, tc.err, got, tc.want)
			}
		})
	}
}

// maxRand returns the top of the jitter range, so the test sees the cap; zeroRand the bottom.
func maxRand(n int64) int64 { return n - 1 }
func zeroRand(int64) int64  { return 0 }

func TestBackoffFullJitterBounds(t *testing.T) {
	base, maxDelay := time.Second, 5*time.Minute
	tests := []struct {
		attempt int
		wantCap time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{9, 256 * time.Second},
		{10, 5 * time.Minute}, // 512s capped at 300s
		{64, 5 * time.Minute}, // no overflow
		{0, 1 * time.Second},  // treated as the first attempt
	}
	for _, tc := range tests {
		upper := Backoff(tc.attempt, base, maxDelay, 0, maxRand)
		lower := Backoff(tc.attempt, base, maxDelay, 0, zeroRand)
		if upper != tc.wantCap {
			t.Errorf("attempt %d: upper bound %v, want %v", tc.attempt, upper, tc.wantCap)
		}
		if lower != 0 {
			t.Errorf("attempt %d: full jitter must reach 0, got %v", tc.attempt, lower)
		}
	}
}

func TestBackoffIsRandomWithinRange(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := range 200 {
		n := int64(i)
		d := Backoff(4, time.Second, time.Minute, 0, func(m int64) int64 { return (n * 7919) % m })
		if d < 0 || d > 8*time.Second {
			t.Fatalf("delay %v outside [0, 8s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Fatalf("jitter produced only %d distinct delays", len(seen))
	}
}

func TestBackoffRetryAfterFloor(t *testing.T) {
	// Jitter picked 0, but the provider asked for 30s: honour it (FR-021).
	if d := Backoff(1, time.Second, time.Minute, 30*time.Second, zeroRand); d != 30*time.Second {
		t.Fatalf("got %v, want the Retry-After floor of 30s", d)
	}
	// Jitter is larger than Retry-After: keep jitter.
	if d := Backoff(3, time.Second, time.Minute, time.Second, maxRand); d != 4*time.Second {
		t.Fatalf("got %v, want 4s", d)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"2", 2 * time.Second},
		{"  120 ", 2 * time.Minute},
		{"-5", 0},
		{"soon", 0},
		{"Wed, 18 Nov 2026 10:00:30 GMT", 30 * time.Second}, // IMF-fixdate
		{"Wednesday, 18-Nov-26 10:01:00 GMT", time.Minute},  // RFC 850
		{"Wed Nov 18 10:00:05 2026", 5 * time.Second},       // ANSI C
		{"Wed, 18 Nov 2026 09:00:00 GMT", 0},                // in the past
	}
	for _, tc := range tests {
		if got := ParseRetryAfter(tc.in, now); got != tc.want {
			t.Errorf("ParseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestShouldFail(t *testing.T) {
	tests := []struct {
		attempts, max int
		want          bool
	}{
		{1, 5, false},
		{4, 5, false},
		{5, 5, true}, // the 5th failed attempt is the last (attempts count the first)
		{6, 5, true},
		{1, 1, true},
	}
	for _, tc := range tests {
		if got := ShouldFail(tc.attempts, tc.max); got != tc.want {
			t.Errorf("ShouldFail(%d, %d) = %v", tc.attempts, tc.max, got)
		}
	}
}
