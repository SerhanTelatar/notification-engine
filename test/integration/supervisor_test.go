//go:build integration

package integration

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
)

// US7-AS4, quickstart #15: losing RabbitMQ must not stop the worker. The supervisor reports not
// ready while the broker is gone, keeps running, and starts a fresh session once it's back.
func TestSupervisorSurvivesBrokerOutage(t *testing.T) {
	u, err := url.Parse(env.AMQPURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newToggleProxy(t, u.Host)
	u.Host = proxy.addr

	var sessions atomic.Int32
	sup := rabbitmq.NewSupervisor(rabbitmq.SupervisorConfig{
		URL: u.String(), Name: "test-supervisor", Log: quietLog,
		MinBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
	}, func(ctx context.Context, conn *amqp.Connection) error {
		sessions.Add(1)
		<-ctx.Done() // a session runs until its connection is lost or the worker stops
		return nil
	})
	alive := sup.Alive()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, 10*time.Second, "first session ready", func() bool { return sessions.Load() == 1 && alive(ctx) == nil })

	proxy.Stop() // the broker disappears
	waitFor(t, 10*time.Second, "not ready without the broker", func() bool { return alive(ctx) != nil })
	select {
	case err := <-done:
		t.Fatalf("supervisor exited during the outage: %v", err)
	case <-time.After(500 * time.Millisecond): // still running and redialling
	}

	proxy.Start() // the broker is back on the same address
	waitFor(t, 10*time.Second, "a new session after recovery", func() bool { return sessions.Load() == 2 && alive(ctx) == nil })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor didn't stop after cancel")
	}
	if alive(context.Background()) == nil {
		t.Fatal("must not report ready after stopping")
	}
}
