// Package noop provides pass-through port implementations. They're wired until the real
// adapters land: the Redis limiter (US3), the gobreaker breaker (US7), the Redis broadcaster
// (US9) and the Prometheus recorder (US7). The clock and the UUIDv7 generator are permanent.
package noop

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Compile-time interface checks.
var (
	_ ports.RateLimiter       = Limiter{}
	_ ports.CircuitBreaker    = Breaker{}
	_ ports.StatusBroadcaster = Broadcaster{}
	_ ports.MetricsRecorder   = Recorder{}
	_ ports.Clock             = Clock{}
	_ ports.IDGenerator       = UUIDv7{}
)

// Limiter grants every permit immediately.
type Limiter struct{}

// Acquire returns at once unless ctx is already done.
func (Limiter) Acquire(ctx context.Context, _ domain.Channel) (time.Time, error) {
	return time.Now(), ctx.Err() //nolint:wrapcheck // ctx error is self-describing
}

// Breaker is permanently closed.
type Breaker struct{}

// Allow always permits.
func (Breaker) Allow(domain.Channel) error { return nil }

// Call always runs fn.
func (Breaker) Call(ctx context.Context, _ domain.Channel, fn func(context.Context) ports.ProviderResult) (ports.ProviderResult, error) {
	return fn(ctx), nil
}

// Broadcaster discards status events.
type Broadcaster struct{}

// Broadcast does nothing.
func (Broadcaster) Broadcast(context.Context, ports.StatusEvent) error { return nil }

// Recorder discards measurements.
type Recorder struct{}

func (Recorder) Delivered(domain.Channel, domain.Outcome, time.Duration) {}
func (Recorder) Retried(domain.Channel)                                  {}
func (Recorder) DeadLettered(domain.Channel, string)                     {}
func (Recorder) EndToEnd(domain.Channel, domain.Priority, time.Duration) {}
func (Recorder) LimiterWait(domain.Channel, time.Duration)               {}
func (Recorder) LimiterTimeout(domain.Channel)                           {}
func (Recorder) RelayPublished(domain.Channel, int)                      {}
func (Recorder) RelaySkipped(domain.Channel)                             {}
func (Recorder) Reaped(string, int)                                      {}
func (Recorder) Backlog([]ports.BacklogCount)                            {}
func (Recorder) QueueDepth(string, int)                                  {}

// Clock is the system clock in UTC.
type Clock struct{}

// Now returns the current UTC time.
func (Clock) Now() time.Time { return time.Now().UTC() }

// UUIDv7 generates time-ordered UUIDs (data-model.md notifications.id).
type UUIDv7 struct{}

// NewID returns a new UUIDv7 string. uuid.NewV7 fails only if the OS entropy source fails;
// a random v4 is then still a valid, unique id.
func (UUIDv7) NewID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
