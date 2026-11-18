package provider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

func result(o domain.Outcome) func(context.Context) ports.ProviderResult {
	return func(context.Context) ports.ProviderResult { return ports.ProviderResult{Outcome: o} }
}

func TestBreakerTripsOnConsecutiveTransientFailuresOnly(t *testing.T) {
	var mu sync.Mutex
	var states []int
	b := NewBreakers(3, 100*time.Millisecond, func(ch domain.Channel, s int) {
		if ch == domain.ChannelSMS {
			mu.Lock()
			states = append(states, s)
			mu.Unlock()
		}
	})
	ctx := context.Background()

	// Permanent failures never trip it, and a success resets the streak.
	for range 10 {
		if _, err := b.Call(ctx, domain.ChannelSMS, result(domain.OutcomePermanent)); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = b.Call(ctx, domain.ChannelSMS, result(domain.OutcomeTransient))
	_, _ = b.Call(ctx, domain.ChannelSMS, result(domain.OutcomeTransient))
	_, _ = b.Call(ctx, domain.ChannelSMS, result(domain.OutcomeSuccess))
	if b.Allow(domain.ChannelSMS) != nil {
		t.Fatal("2 transient failures then a success must not trip a 3-failure breaker")
	}

	// Three in a row: open. The transient result itself is still returned.
	for range 3 {
		res, err := b.Call(ctx, domain.ChannelSMS, result(domain.OutcomeTransient))
		if err != nil || res.Outcome != domain.OutcomeTransient {
			t.Fatalf("a counted failure is still a normal result: %+v %v", res, err)
		}
	}
	err := b.Allow(domain.ChannelSMS)
	var open *ports.BreakerOpenError
	if !errors.As(err, &open) || open.RetryIn <= 0 || open.RetryIn > 100*time.Millisecond {
		t.Fatalf("Allow = %v, want BreakerOpenError with the remaining cool-down", err)
	}
	called := false
	if _, err := b.Call(ctx, domain.ChannelSMS, func(context.Context) ports.ProviderResult { called = true; return ports.ProviderResult{} }); !errors.Is(err, ports.ErrBreakerOpen) || called {
		t.Fatalf("an open breaker must reject without calling: %v called=%v", err, called)
	}
	// Channels are independent.
	if b.Allow(domain.ChannelEmail) != nil {
		t.Fatal("email must be unaffected by sms failures")
	}

	// After the cool-down one trial goes through; success closes the breaker.
	time.Sleep(120 * time.Millisecond)
	if _, err := b.Call(ctx, domain.ChannelSMS, result(domain.OutcomeSuccess)); err != nil {
		t.Fatalf("trial call: %v", err)
	}
	if b.Allow(domain.ChannelSMS) != nil {
		t.Fatal("a successful trial must close the breaker")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []int{StateClosed, StateOpen, StateHalfOpen, StateClosed}
	if len(states) != len(want) {
		t.Fatalf("state changes %v, want %v", states, want)
	}
	for i := range want {
		if states[i] != want[i] {
			t.Fatalf("state changes %v, want %v", states, want)
		}
	}
}

func TestBreakerFailedTrialReopens(t *testing.T) {
	b := NewBreakers(1, 50*time.Millisecond, nil)
	ctx := context.Background()
	_, _ = b.Call(ctx, domain.ChannelPush, result(domain.OutcomeTransient))
	if b.Allow(domain.ChannelPush) == nil {
		t.Fatal("must be open")
	}
	time.Sleep(60 * time.Millisecond)
	_, _ = b.Call(ctx, domain.ChannelPush, result(domain.OutcomeTransient)) // failed trial
	if b.Allow(domain.ChannelPush) == nil {
		t.Fatal("a failed trial must reopen the breaker for another cool-down")
	}
}
