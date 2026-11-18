// Package redis holds the Redis adapters. Redis is never a source of truth (Constitution III):
// losing its state at most allows one extra permit right after a restart.
package redis

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

//go:embed gcra.lua
var gcraSource string

// defaultMaxWait applies when the caller's context has no deadline.
const defaultMaxWait = 30 * time.Second

// RateLimiter implements ports.RateLimiter as a distributed GCRA with zero burst (research R8).
// Every worker shares one key per channel, so the limit holds across all of them combined
// (FR-016, SC-003).
type RateLimiter struct {
	client   goredis.UniversalClient
	interval time.Duration
	script   *goredis.Script
}

var _ ports.RateLimiter = (*RateLimiter)(nil)

// NewRateLimiter allows at most perSecond deliveries per channel. headroom (e.g. 0.05) spaces
// slots that much wider than 1s/perSecond, so millisecond scheduling and network jitter between
// a slot and the provider can't push a sliding second over the limit (SC-003). 0 = exact spacing.
func NewRateLimiter(client goredis.UniversalClient, perSecond int, headroom float64) *RateLimiter {
	interval := float64(time.Second) / float64(max(1, perSecond)) * (1 + max(0, headroom))
	return &RateLimiter{
		client:   client,
		interval: time.Duration(interval),
		script:   goredis.NewScript(gcraSource),
	}
}

// Key is the Redis key for a channel's schedule.
func Key(ch domain.Channel) string { return "ratelimit:" + string(ch) }

// Acquire reserves the next free slot for ch, sleeps until it and returns the slot's local time,
// so the caller can pace its provider call on it. The reservation never reaches past ctx's deadline: when the next slot is further away,
// Acquire waits for the deadline and returns ctx's error without reserving anything. A Redis
// failure is returned at once; the caller retries or gives up, and never proceeds without a permit
// (fail closed).
func (l *RateLimiter) Acquire(ctx context.Context, ch domain.Channel) (time.Time, error) {
	maxWait := defaultMaxWait
	if dl, ok := ctx.Deadline(); ok {
		maxWait = time.Until(dl)
	}
	if maxWait <= 0 {
		// The deadline has passed, but ctx.Err() can still be nil for a moment, until the
		// context's own timer fires. Never return (zero, nil): that would read as a granted permit
		// and fail open.
		return time.Time{}, deadlineErr(ctx)
	}
	res, err := l.script.Run(ctx, l.client, []string{Key(ch)}, l.interval.Microseconds(), maxWait.Microseconds()).Int64()
	replied := time.Now()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return time.Time{}, ctxErr //nolint:wrapcheck // the caller's own context error
		}
		return time.Time{}, fmt.Errorf("rate limiter for %s: %w", ch, err)
	}
	if res < 0 {
		// No slot within the caller's patience: don't reserve one it would abandon.
		<-ctx.Done()
		return time.Time{}, ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
	slot := replied.Add(time.Duration(res) * time.Microsecond)
	if res == 0 {
		return slot, nil
	}
	timer := time.NewTimer(time.Until(slot))
	defer timer.Stop()
	select {
	case <-timer.C:
		return slot, nil
	case <-ctx.Done():
		// The reserved slot goes unused: a little lost throughput, never an extra send.
		return time.Time{}, ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
}

// deadlineErr is ctx's error once its deadline has passed, never nil.
func deadlineErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // the caller's own context error
	}
	return context.DeadlineExceeded
}

// Ping is a readiness check.
func Ping(client goredis.UniversalClient) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis ping: %w", err)
		}
		return nil
	}
}
