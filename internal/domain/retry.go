package domain

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Classify maps a provider response to an outcome (FR-020, research R9). status is 0 when no
// response arrived; err is any transport error (timeout, refused, DNS, reset).
//
//   - transport error            → transient
//   - 2xx                        → success (the contract expects 202; webhook.site returns 200)
//   - 429, 5xx                   → transient
//   - anything else (4xx, 1xx, 3xx with redirects disabled) → permanent
func Classify(status int, err error) Outcome {
	switch {
	case err != nil:
		return OutcomeTransient
	case status >= 200 && status < 300:
		return OutcomeSuccess
	case status == 429, status >= 500 && status < 600:
		return OutcomeTransient
	default:
		return OutcomePermanent
	}
}

// Backoff returns the delay before the next attempt, using full jitter:
//
//	delay = rnd(0 … min(maxDelay, base·2^(attempt−1)))
//
// then raised to retryAfter when the provider asked for longer (FR-021). attempt is the 1-based
// number of the attempt that just failed. rnd(n) must return a value in [0, n), e.g.
// math/rand/v2.Int64N.
func Backoff(attempt int, base, maxDelay, retryAfter time.Duration, rnd func(n int64) int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := maxDelay
	// base·2^(attempt−1) without overflow: stop doubling once the cap is reached.
	if exp := attempt - 1; exp < 62 {
		if grown := float64(base) * math.Pow(2, float64(exp)); grown < float64(maxDelay) {
			ceiling = time.Duration(grown)
		}
	}
	delay := time.Duration(0)
	if ceiling > 0 {
		delay = time.Duration(rnd(int64(ceiling) + 1)) // inclusive of the ceiling
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	return delay
}

// ShouldFail reports whether a failed attempt was the last one allowed. attempts counts every
// provider call so far, including the first (data-model.md notifications.attempts).
func ShouldFail(attempts, maxAttempts int) bool { return attempts >= maxAttempts }

// HTTP date layouts accepted for Retry-After (RFC 9110 §5.6.7), as net/http.ParseTime does.
var httpDateLayouts = []string{
	"Mon, 02 Jan 2006 15:04:05 GMT",  // IMF-fixdate
	"Monday, 02-Jan-06 15:04:05 GMT", // RFC 850
	"Mon Jan _2 15:04:05 2006",       // ANSI C asctime
}

// ParseRetryAfter reads a Retry-After value given as delay-seconds or an HTTP date. Missing,
// invalid, negative or past values give 0.
func ParseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	for _, layout := range httpDateLayouts {
		if at, err := time.Parse(layout, v); err == nil {
			if d := at.Sub(now); d > 0 {
				return d
			}
			return 0
		}
	}
	return 0
}
