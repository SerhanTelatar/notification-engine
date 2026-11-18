//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

func TestRelayUnconfirmedPublishRollsBack(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	t.Cleanup(func() { resetBroker(t) }) // restore the deleted exchange for later tests

	conn, err := rabbitmq.Dial(env.AMQPURL, "test-relay")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	repo := postgres.NewNotificationRepo(env.Pool)
	pub := rabbitmq.NewPublisher(conn, 2*time.Second)
	defer pub.Close()
	relay := app.NewRelay(app.RelayDeps{Repo: repo, Publisher: pub, Events: noop.Broadcaster{}, Metrics: newCountingMetrics(),
		Clock: noop.Clock{}, IDs: noop.UUIDv7{}, Log: quietLog, MaxInflight: 1000, BatchSize: 500})

	n := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))

	// Delete the exchange: the broker can no longer confirm the publish.
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDelete(rabbitmq.Exchange, false, false); err != nil {
		t.Fatal(err)
	}
	_ = ch.Close()

	if err := relay.Tick(context.Background()); err == nil {
		t.Fatal("an unconfirmed publish must fail the relay pass")
	}
	if st, _ := statusOf(t, n.ID); st != "pending" {
		t.Fatalf("status %s: the transaction must roll back so the row stays due", st)
	}

	// Once the broker is back, the next pass publishes it.
	resetBroker(t)
	if err := relay.Tick(context.Background()); err != nil {
		t.Fatalf("relay after recovery: %v", err)
	}
	if st, _ := statusOf(t, n.ID); st != "queued" || queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)) != 1 {
		t.Fatalf("status %s depth %d after recovery", st, queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)))
	}
}

// insertBurst bulk-inserts n due notifications for a channel (much faster than n creates).
func insertBurst(t testing.TB, ch domain.Channel, recipient string, n int) {
	t.Helper()
	_, err := env.Pool.Exec(context.Background(), `
		INSERT INTO notifications (id, payload_hash, channel, recipient, content, priority, status,
		                           max_attempts, next_attempt_at, correlation_id)
		SELECT gen_random_uuid(), sha256(($1 || i)::bytea), $1, $2, 'burst', 2, 'pending', 5, now(), 'burst-' || $1
		FROM generate_series(1, $3) AS i`, string(ch), recipient, n)
	if err != nil {
		t.Fatalf("insert burst: %v", err)
	}
}

func TestRelayBackpressureBurst(t *testing.T) {
	const (
		burst = 5000
		cap   = 1000
	)
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	// 40 ms per call with 20 consumers drains about 500/s, far slower than the relay can publish
	// (500 per 50 ms tick), so the backlog has to pile up against the cap.
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: 202, Delay: 40 * time.Millisecond}
	})
	cfg := defaultPipeline(mock.SendURL())
	cfg.relays = 2 // two worker replicas
	cfg.maxInflight = cap
	cfg.concurrency = 20
	// Anything queued longer than this is republished and counted. At the nominal ~500/s a row
	// waits about 2 s behind the 1,000 cap; 15 s leaves room for a loaded CI runner (-race, other
	// packages in parallel) while still catching a message that is really stuck.
	cfg.queuedTimeout = 15 * time.Second
	cfg.reaperInterval = 500 * time.Millisecond

	insertBurst(t, domain.ChannelSMS, "+905551234567", burst)
	insertBurst(t, domain.ChannelEmail, "ada@example.com", 100)

	// Sample the sms queued count while the burst drains.
	var maxQueued atomic.Int64
	sampleCtx, stopSampling := context.WithCancel(context.Background())
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for sampleCtx.Err() == nil {
			var q int64
			if err := env.Pool.QueryRow(sampleCtx, `SELECT count(*) FROM notifications WHERE channel = 'sms' AND status = 'queued'`).Scan(&q); err == nil && q > maxQueued.Load() {
				maxQueued.Store(q)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	p := startPipeline(t, cfg)
	start := time.Now()
	waitFor(t, 15*time.Second, "email delivered while sms is backlogged", func() bool {
		var n int
		_ = env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE channel = 'email' AND status = 'sent'`).Scan(&n)
		return n == 100
	})
	emailDone := time.Since(start)
	waitFor(t, 120*time.Second, "the whole burst delivered", func() bool { return countStatus(t, "sent") == burst+100 })
	stopSampling()
	sampler.Wait()

	t.Logf("burst of %d sms + 100 email drained in %v (email done after %v); max sms queued %d",
		burst, time.Since(start).Round(time.Millisecond), emailDone.Round(time.Millisecond), maxQueued.Load())
	if maxQueued.Load() > cap {
		t.Fatalf("sms queued peaked at %d, above RELAY_MAX_INFLIGHT_PER_CHANNEL=%d", maxQueued.Load(), cap)
	}
	if maxQueued.Load() < cap*9/10 {
		t.Fatalf("sms queued peaked at only %d; the backlog never reached the cap, so the test isn't measuring backpressure", maxQueued.Load())
	}
	if got := p.metrics.get("reaped:queued"); got != 0 {
		t.Fatalf("reaper republished %d legitimately waiting messages", got)
	}
	// Backpressure shows as the peak sitting at the cap, not above it. Skips are rare in steady
	// state: consumers keep moving rows to sending, so each tick usually tops up with a small
	// partial quota instead of skipping.
	t.Logf("relay backpressure skips: %d", p.metrics.get("relay_skipped"))
	if got := len(mock.Received()); got != burst+100 {
		t.Fatalf("provider calls %d, want %d (no duplicates, nothing lost)", got, burst+100)
	}
}
