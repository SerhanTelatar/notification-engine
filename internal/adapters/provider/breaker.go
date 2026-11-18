package provider

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Breaker states as reported to circuit_breaker_state (contracts/observability.md).
const (
	StateClosed   = 0
	StateHalfOpen = 1
	StateOpen     = 2
)

// errTransient marks a provider call that counts against the breaker.
var errTransient = errors.New("transient provider failure")

// Breakers implements ports.CircuitBreaker with one sony/gobreaker breaker per channel (FR-024,
// research R9), so an outage on one channel never pauses another. State is per worker process: N
// workers may each send one trial call per cool-down (ADR 0005).
type Breakers struct {
	cooldown time.Duration
	mu       sync.Mutex
	byCh     map[domain.Channel]*gobreaker.CircuitBreaker[ports.ProviderResult]
	openedAt map[domain.Channel]time.Time
	now      func() time.Time
}

var _ ports.CircuitBreaker = (*Breakers)(nil)

// NewBreakers trips a channel after consecutive transient failures and keeps it open for
// cooldown, then lets one trial call through (half-open). Permanent failures (4xx other than 429)
// count as successes: a bad recipient isn't a provider outage. onState is called on every state
// change with StateClosed/StateHalfOpen/StateOpen.
func NewBreakers(consecutive int, cooldown time.Duration, onState func(domain.Channel, int)) *Breakers {
	b := &Breakers{cooldown: cooldown, openedAt: map[domain.Channel]time.Time{},
		byCh: map[domain.Channel]*gobreaker.CircuitBreaker[ports.ProviderResult]{}, now: time.Now}
	for _, ch := range domain.Channels {
		b.byCh[ch] = gobreaker.NewCircuitBreaker[ports.ProviderResult](gobreaker.Settings{
			Name:        string(ch),
			MaxRequests: 1, // one trial call while half-open
			Timeout:     cooldown,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= uint32(max(1, consecutive)) }, //nolint:gosec // small positive config value
			IsSuccessful: func(err error) bool {
				return !errors.Is(err, errTransient)
			},
			OnStateChange: func(_ string, _, to gobreaker.State) {
				b.mu.Lock()
				if to == gobreaker.StateOpen {
					b.openedAt[ch] = b.now()
				}
				b.mu.Unlock()
				if onState != nil {
					onState(ch, stateValue(to))
				}
			},
		})
		if onState != nil {
			onState(ch, StateClosed)
		}
	}
	return b
}

// Allow reports, without calling the provider, whether ch's breaker currently rejects calls.
func (b *Breakers) Allow(ch domain.Channel) error {
	cb, ok := b.byCh[ch]
	if !ok {
		return nil
	}
	if cb.State() == gobreaker.StateOpen {
		return b.openError(ch)
	}
	return nil
}

// Call runs fn through ch's breaker. A transient outcome counts as a failure; the result is still
// returned so the caller can record the attempt. When the breaker rejects the call, fn isn't run
// and the error is a *ports.BreakerOpenError with the remaining cool-down.
func (b *Breakers) Call(ctx context.Context, ch domain.Channel, fn func(context.Context) ports.ProviderResult) (ports.ProviderResult, error) {
	cb, ok := b.byCh[ch]
	if !ok {
		return fn(ctx), nil
	}
	res, err := cb.Execute(func() (ports.ProviderResult, error) {
		r := fn(ctx)
		if r.Outcome == domain.OutcomeTransient {
			return r, errTransient
		}
		return r, nil
	})
	switch {
	case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
		return ports.ProviderResult{}, b.openError(ch)
	case errors.Is(err, errTransient):
		return res, nil // a counted failure, but a normal result for the caller
	}
	return res, nil //nolint:nilerr // gobreaker only returns the sentinels handled above
}

// RetryIn is how long until ch's breaker allows its next trial call (0 when it isn't open).
func (b *Breakers) RetryIn(ch domain.Channel) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	opened, ok := b.openedAt[ch]
	if !ok {
		return 0
	}
	return max(0, b.cooldown-b.now().Sub(opened))
}

func (b *Breakers) openError(ch domain.Channel) error {
	retry := b.RetryIn(ch)
	if retry <= 0 {
		retry = time.Second // half-open and busy: the trial call decides shortly
	}
	return &ports.BreakerOpenError{Channel: ch, RetryIn: retry}
}

func stateValue(s gobreaker.State) int {
	switch s {
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateClosed
	}
}
