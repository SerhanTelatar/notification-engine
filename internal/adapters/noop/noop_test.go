package noop

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

func TestUUIDv7IsTimeOrdered(t *testing.T) {
	g := UUIDv7{}
	prev := g.NewID()
	for range 1000 {
		next := g.NewID()
		u, err := uuid.Parse(next)
		if err != nil || u.Version() != 7 {
			t.Fatalf("not a UUIDv7: %s (%v)", next, err)
		}
		if next <= prev {
			t.Fatalf("ids must sort by creation: %s <= %s", next, prev)
		}
		prev = next
	}
}

func TestPassThroughs(t *testing.T) {
	ctx := context.Background()
	if err := (Limiter{}).Acquire(ctx, domain.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if (Limiter{}).Acquire(cancelled, domain.ChannelSMS) == nil {
		t.Fatal("a cancelled ctx must be reported")
	}
	called := false
	res, err := Breaker{}.Call(ctx, domain.ChannelSMS, func(context.Context) ports.ProviderResult {
		called = true
		return ports.ProviderResult{Outcome: domain.OutcomeSuccess}
	})
	if err != nil || !called || res.Outcome != domain.OutcomeSuccess || (Breaker{}).Allow(domain.ChannelSMS) != nil {
		t.Fatal("breaker must always pass through")
	}
	if (Clock{}).Now().Location().String() != "UTC" {
		t.Fatal("clock must be UTC")
	}
}
