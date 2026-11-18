//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/provider"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// The worker pipeline assembled from real adapters, the way cmd/worker wires it, but with
// test-sized timings. The breaker and broadcaster are pass-throughs until US7 and US9; the rate
// limiter is a pass-through unless a test supplies one.

type pipelineConfig struct {
	providerURL     string
	providerTimeout time.Duration
	relays          int // relay instances (simulates worker replicas)
	relayInterval   time.Duration
	maxInflight     int
	batch           int
	concurrency     int
	reaperInterval  time.Duration
	sendingTimeout  time.Duration
	queuedTimeout   time.Duration
	backoffBase     time.Duration
	backoffMax      time.Duration
	limiter         ports.RateLimiter // nil = pass-through
	limiterMaxWait  time.Duration     // 0 = 1 s
}

func defaultPipeline(providerURL string) pipelineConfig {
	return pipelineConfig{
		providerURL: providerURL, providerTimeout: 2 * time.Second,
		relays: 1, relayInterval: 50 * time.Millisecond, maxInflight: 1000, batch: 500, concurrency: 10,
		reaperInterval: 200 * time.Millisecond, sendingTimeout: time.Minute, queuedTimeout: 15 * time.Minute,
		backoffBase: 50 * time.Millisecond, backoffMax: 200 * time.Millisecond,
	}
}

type pipeline struct {
	t       testing.TB
	conn    *amqp.Connection
	repo    *postgres.NotificationRepo
	pub     *rabbitmq.Publisher
	creator *app.Creator
	metrics *countingMetrics
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// resetBroker declares the topology and empties every queue.
func resetBroker(t testing.TB) {
	t.Helper()
	conn, err := rabbitmq.Dial(env.AMQPURL, "test-reset")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := rabbitmq.DeclareTopology(conn); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	for _, c := range domain.Channels {
		_, _ = ch.QueuePurge(rabbitmq.QueueName(c), false)
	}
	_, _ = ch.QueuePurge(rabbitmq.DLQ, false)
}

// queueDepth reads a queue's ready-message count.
func queueDepth(t testing.TB, queue string) int {
	t.Helper()
	conn, err := rabbitmq.Dial(env.AMQPURL, "test-depth")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspect %s: %v", queue, err)
	}
	return q.Messages
}

func startPipeline(t testing.TB, cfg pipelineConfig) *pipeline {
	t.Helper()
	conn, err := rabbitmq.Dial(env.AMQPURL, "test-worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := rabbitmq.DeclareTopology(conn); err != nil {
		t.Fatal(err)
	}
	p := &pipeline{t: t, conn: conn, repo: postgres.NewNotificationRepo(env.Pool),
		pub: rabbitmq.NewPublisher(conn, 0), metrics: newCountingMetrics()}
	p.creator = app.NewCreator(app.CreatorDeps{Repo: p.repo, IDs: noop.UUIDv7{}, Clock: noop.Clock{},
		Events: noop.Broadcaster{}, Log: quietLog, MaxAttempts: 5})

	deliverer := app.NewDeliverer(app.DelivererDeps{
		Repo: p.repo, Provider: provider.NewClient(cfg.providerURL, cfg.providerTimeout, nil),
		Breaker: noop.Breaker{}, Limiter: limiterOrNoop(cfg.limiter), Events: noop.Broadcaster{}, Metrics: p.metrics,
		Clock: noop.Clock{}, Log: quietLog, BackoffBase: cfg.backoffBase, BackoffMax: cfg.backoffMax,
		LimiterMaxWait: durationOr(cfg.limiterMaxWait, time.Second),
	})
	reaper := app.NewReaper(app.ReaperDeps{Repo: p.repo, Publisher: p.pub, Events: noop.Broadcaster{},
		Metrics: p.metrics, Clock: noop.Clock{}, Log: quietLog,
		SendingTimeout: cfg.sendingTimeout, QueuedTimeout: cfg.queuedTimeout})

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	run := func(f func(context.Context)) {
		p.wg.Add(1)
		go func() { defer p.wg.Done(); f(ctx) }()
	}
	every := func(interval time.Duration, tick func(context.Context) error) {
		run(func(ctx context.Context) {
			tk := time.NewTicker(interval)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
					_ = tick(ctx)
				}
			}
		})
	}
	for range max(1, cfg.relays) {
		relay := app.NewRelay(app.RelayDeps{Repo: p.repo, Publisher: p.pub, Events: noop.Broadcaster{},
			Metrics: p.metrics, Clock: noop.Clock{}, IDs: noop.UUIDv7{}, Log: quietLog,
			MaxInflight: cfg.maxInflight, BatchSize: cfg.batch})
		every(cfg.relayInterval, relay.Tick)
	}
	every(cfg.reaperInterval, reaper.Tick)
	for _, ch := range domain.Channels {
		c := rabbitmq.NewConsumer(conn, ch, cfg.concurrency, deliverer.Handle, quietLog)
		run(func(ctx context.Context) {
			if err := c.Run(ctx); err != nil {
				t.Errorf("consumer %s: %v", ch, err)
			}
		})
	}
	t.Cleanup(p.stop)
	return p
}

func limiterOrNoop(l ports.RateLimiter) ports.RateLimiter {
	if l == nil {
		return noop.Limiter{}
	}
	return l
}

func durationOr(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// stop cancels every component and waits for them, like SIGTERM on the worker.
func (p *pipeline) stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	p.wg.Wait()
	p.pub.Close()
	_ = p.conn.Close()
	p.cancel = nil
}

func (p *pipeline) create(t testing.TB, req app.CreateRequest) *domain.Notification {
	t.Helper()
	if req.Channel == "" {
		req.Channel, req.Recipient = "sms", "+905551234567"
	}
	if req.Content == nil {
		c := "hello"
		req.Content = &c
	}
	if req.CorrelationID == "" {
		req.CorrelationID = "it-correlation"
	}
	res, err := p.creator.Create(context.Background(), req)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return res.Notification
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func countStatus(t testing.TB, status string) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE status = $1`, status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type attemptRow struct {
	no       int
	outcome  string
	status   *int
	errText  *string
	started  time.Time
	finished time.Time
}

func attemptsFor(t testing.TB, id string) []attemptRow {
	t.Helper()
	rows, err := env.Pool.Query(context.Background(), `SELECT attempt_no, outcome, http_status, error, started_at, finished_at
		FROM notification_attempts WHERE notification_id = $1 ORDER BY attempt_no`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []attemptRow
	for rows.Next() {
		var a attemptRow
		if err := rows.Scan(&a.no, &a.outcome, &a.status, &a.errText, &a.started, &a.finished); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// countingMetrics is a ports.MetricsRecorder that counts calls.
type countingMetrics struct {
	mu sync.Mutex
	c  map[string]int
}

func newCountingMetrics() *countingMetrics { return &countingMetrics{c: map[string]int{}} }

func (m *countingMetrics) add(k string, n int) { m.mu.Lock(); m.c[k] += n; m.mu.Unlock() }
func (m *countingMetrics) get(k string) int    { m.mu.Lock(); defer m.mu.Unlock(); return m.c[k] }

func (m *countingMetrics) Delivered(ch domain.Channel, o domain.Outcome, _ time.Duration) {
	m.add("delivered:"+string(o), 1)
}
func (m *countingMetrics) Retried(domain.Channel)                                  { m.add("retried", 1) }
func (m *countingMetrics) DeadLettered(_ domain.Channel, reason string)            { m.add("dead:"+reason, 1) }
func (m *countingMetrics) EndToEnd(domain.Channel, domain.Priority, time.Duration) {}
func (m *countingMetrics) LimiterWait(domain.Channel, time.Duration)               {}
func (m *countingMetrics) LimiterTimeout(domain.Channel)                           { m.add("limiter_timeout", 1) }
func (m *countingMetrics) RelayPublished(_ domain.Channel, n int)                  { m.add("relay_published", n) }
func (m *countingMetrics) RelaySkipped(domain.Channel)                             { m.add("relay_skipped", 1) }
func (m *countingMetrics) Reaped(kind string, n int)                               { m.add("reaped:"+kind, n) }
func (m *countingMetrics) Backlog([]ports.BacklogCount)                            {}
