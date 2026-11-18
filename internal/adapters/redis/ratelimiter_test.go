package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// lateCtx models the moment right after a deadline passes: Deadline() is in the past but the
// context's timer hasn't fired yet, so Err() is still nil.
type lateCtx struct{ context.Context }

func (lateCtx) Deadline() (time.Time, bool) { return time.Now().Add(-time.Millisecond), true }
func (lateCtx) Err() error                  { return nil }

func TestAcquireNeverFailsOpenAtTheDeadline(t *testing.T) {
	l := NewRateLimiter(nil, 100, 0) // the client must not even be reached
	slot, err := l.Acquire(lateCtx{context.Background()}, domain.ChannelSMS)
	if err == nil {
		t.Fatalf("Acquire returned (%v, nil) after the deadline: a missing permit must be an error", slot)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

func TestIntervalIncludesHeadroom(t *testing.T) {
	tests := []struct {
		perSecond int
		headroom  float64
		want      time.Duration
	}{
		{100, 0, 10 * time.Millisecond},
		{100, 0.05, 10500 * time.Microsecond},
		{20, 0.10, 55 * time.Millisecond},
		{0, 0, time.Second},              // degenerate limit treated as 1/s
		{10, -1, 100 * time.Millisecond}, // negative headroom ignored
	}
	for _, tc := range tests {
		if got := NewRateLimiter(nil, tc.perSecond, tc.headroom).interval; got != tc.want {
			t.Errorf("NewRateLimiter(%d, %g).interval = %v, want %v", tc.perSecond, tc.headroom, got, tc.want)
		}
	}
}
