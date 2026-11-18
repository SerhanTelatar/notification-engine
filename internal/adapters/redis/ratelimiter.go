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

// NewRateLimiter allows perSecond deliveries per channel.
func NewRateLimiter(client goredis.UniversalClient, perSecond int) *RateLimiter {
	return &RateLimiter{
		client:   client,
		interval: time.Second / time.Duration(max(1, perSecond)),
		script:   goredis.NewScript(gcraSource),
	}
}

// Key is the Redis key for a channel's schedule.
func Key(ch domain.Channel) string { return "ratelimit:" + string(ch) }

// Acquire reserves the next free slot for ch and sleeps until it, so the caller proceeds exactly on
// its slot. The reservation never reaches past ctx's deadline: when the next slot is further away,
// Acquire waits for the deadline and returns ctx's error without reserving anything. A Redis
// failure is returned at once; the caller retries or gives up, and never proceeds without a permit
// (fail closed).
func (l *RateLimiter) Acquire(ctx context.Context, ch domain.Channel) error {
	maxWait := defaultMaxWait
	if dl, ok := ctx.Deadline(); ok {
		maxWait = time.Until(dl)
	}
	if maxWait <= 0 {
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
	res, err := l.script.Run(ctx, l.client, []string{Key(ch)}, l.interval.Microseconds(), maxWait.Microseconds()).Int64()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr //nolint:wrapcheck // the caller's own context error
		}
		return fmt.Errorf("rate limiter for %s: %w", ch, err)
	}
	if res < 0 {
		// No slot within the caller's patience: don't reserve one it would abandon.
		<-ctx.Done()
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
	if res == 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(res) * time.Microsecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// The reserved slot goes unused: a little lost throughput, never an extra send.
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
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
