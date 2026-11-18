// Command worker runs the delivery side: the outbox relay, one consumer per channel (the delivery
// pipeline), the crash-recovery reaper, and an ops server exposing /healthz, /readyz and /metrics
// (Constitution IV). All dependencies are constructed and injected here (Constitution II).
//
// Crash-only by design: Postgres holds every notification durably, so when the broker
// connection is lost the worker exits and is restarted, instead of trying to repair state in
// memory.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	goredis "github.com/redis/go-redis/v9"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/provider"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	redisadapter "github.com/SerhanTelatar/notification-engine/internal/adapters/redis"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/config"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
	"github.com/SerhanTelatar/notification-engine/internal/platform/shutdown"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadWorker(config.FromOS())
	if err != nil {
		return err
	}
	log := logger.New("worker", cfg.LogLevel)

	ctx, stop := shutdown.SignalContext(ctx)
	defer stop()

	// --- Infrastructure -----------------------------------------------------------------------
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, "notification-engine-worker")
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close() // after every component has stopped

	conn, err := rabbitmq.Dial(cfg.AMQPURL, "notification-engine-worker")
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := rabbitmq.DeclareTopology(conn); err != nil {
		return fmt.Errorf("rabbitmq topology: %w", err)
	}
	publisher := rabbitmq.NewPublisher(conn, rabbitmq.DefaultPublishTimeout)
	defer publisher.Close()

	rdb := goredis.NewClient(&goredis.Options{
		Addr: cfg.RedisAddr, DialTimeout: 2 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	defer func() { _ = rdb.Close() }()

	reg := metrics.NewRegistry()
	m := metrics.NewWorker(reg)

	// --- Adapters -----------------------------------------------------------------------------
	repo := postgres.NewNotificationRepo(pool)
	providerClient := provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout,
		func(ch domain.Channel, o domain.Outcome, d time.Duration) {
			m.ProviderDuration.WithLabelValues(string(ch), string(o)).Observe(d.Seconds())
		})
	clock, ids := noop.Clock{}, noop.UUIDv7{}
	// Shared across every worker: at most RATE_LIMIT_PER_SECOND deliveries per channel (FR-016).
	limiter := redisadapter.NewRateLimiter(rdb, cfg.RateLimitPerSecond, cfg.RateLimitHeadroom)
	// One breaker per channel; state changes feed circuit_breaker_state (FR-024).
	breaker := provider.NewBreakers(cfg.BreakerConsecutiveFailures, cfg.BreakerCooldown, func(ch domain.Channel, state int) {
		m.BreakerState.WithLabelValues(string(ch)).Set(float64(state))
	})
	// Pass-throughs until their stories: the Prometheus recorder (US7, T094), the Redis
	// broadcaster (US9).
	events, recorder := noop.Broadcaster{}, noop.Recorder{}

	// --- Use cases ----------------------------------------------------------------------------
	deliverer := app.NewDeliverer(app.DelivererDeps{
		Repo: repo, Provider: providerClient, Breaker: breaker, Limiter: limiter, Events: events,
		Metrics: recorder, Clock: clock, Log: log.With("component", "deliver"),
		BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax, LimiterMaxWait: cfg.LimiterMaxWait,
		SendOffset: cfg.RateLimitSendOffset, MaxDeferAge: cfg.BreakerMaxDeferAge,
	})
	relay := app.NewRelay(app.RelayDeps{
		Repo: repo, Publisher: publisher, Events: events, Metrics: recorder, Clock: clock, IDs: ids,
		Log: log.With("component", "relay"), MaxInflight: cfg.RelayMaxInflightPerChannel, BatchSize: cfg.RelayBatchSize,
	})
	reaper := app.NewReaper(app.ReaperDeps{
		Repo: repo, Publisher: publisher, Events: events, Metrics: recorder, Clock: clock,
		Log: log.With("component", "reaper"), SendingTimeout: cfg.ReaperSendingTimeout, QueuedTimeout: cfg.ReaperQueuedTimeout,
	})

	// --- Readiness: every dependency the worker needs to make progress -----------------------
	checker := health.New(health.DefaultTimeout).
		Require("postgres", postgres.Ping(pool)).
		Require("rabbitmq", amqpAlive(conn)).
		Require("redis", redisadapter.Ping(rdb)) // the limiter fails closed without it

	// --- Components ---------------------------------------------------------------------------
	logTickError := func(component string) func(error) {
		return func(err error) { log.ErrorContext(ctx, "tick failed", "component", component, "error", err) }
	}
	components := []shutdown.Component{
		shutdown.HTTPServer("ops", newOpsServer(cfg.OpsAddr, checker, reg), cfg.ShutdownTimeout),
		shutdown.Every("relay", cfg.RelayInterval, relay.Tick, logTickError("relay")),
		shutdown.Every("reaper", cfg.ReaperInterval, reaper.Tick, logTickError("reaper")),
	}
	for _, ch := range domain.Channels {
		c := rabbitmq.NewConsumer(conn, ch, cfg.ConsumerConcurrencyPerChannel, deliverer.Handle, log)
		components = append(components, shutdown.Component{Name: "consumer-" + string(ch), Run: c.Run})
	}

	log.InfoContext(ctx, "worker starting",
		"ops_addr", cfg.OpsAddr, "provider_timeout", cfg.ProviderTimeout.String(), "rate_limit_per_second", cfg.RateLimitPerSecond,
		"relay_interval", cfg.RelayInterval.String(), "relay_max_inflight_per_channel", cfg.RelayMaxInflightPerChannel,
		"consumer_concurrency_per_channel", cfg.ConsumerConcurrencyPerChannel, "max_backoff", cfg.BackoffMax.String())
	err = shutdown.Run(ctx, log, cfg.ShutdownTimeout, components...)
	log.InfoContext(context.WithoutCancel(ctx), "worker stopped")
	return err
}

// amqpAlive is a readiness check on the broker connection.
func amqpAlive(conn *amqp.Connection) health.Check {
	return func(context.Context) error {
		if conn.IsClosed() {
			return errors.New("rabbitmq connection closed")
		}
		return nil
	}
}

// newOpsServer serves liveness, readiness and Prometheus metrics for the worker.
func newOpsServer(addr string, checker *health.Checker, g prometheus.Gatherer) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LivenessHandler())
	mux.Handle("GET /readyz", checker.ReadinessHandler())
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
}
