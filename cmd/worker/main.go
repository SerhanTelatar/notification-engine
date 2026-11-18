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

	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/provider"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
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

	reg := metrics.NewRegistry()
	m := metrics.NewWorker(reg)

	// --- Adapters -----------------------------------------------------------------------------
	repo := postgres.NewNotificationRepo(pool)
	providerClient := provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout,
		func(ch domain.Channel, o domain.Outcome, d time.Duration) {
			m.ProviderDuration.WithLabelValues(string(ch), string(o)).Observe(d.Seconds())
		})
	clock, ids := noop.Clock{}, noop.UUIDv7{}
	// Pass-throughs until their stories: the Redis limiter (US3), the gobreaker breaker and the
	// Prometheus recorder (US7), the Redis broadcaster (US9).
	limiter, breaker, events, recorder := noop.Limiter{}, noop.Breaker{}, noop.Broadcaster{}, noop.Recorder{}

	// --- Use cases ----------------------------------------------------------------------------
	deliverer := app.NewDeliverer(app.DelivererDeps{
		Repo: repo, Provider: providerClient, Breaker: breaker, Limiter: limiter, Events: events,
		Metrics: recorder, Clock: clock, Log: log.With("component", "deliver"),
		BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax, LimiterMaxWait: cfg.LimiterMaxWait,
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
		Require("rabbitmq", amqpAlive(conn))

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
		"ops_addr", cfg.OpsAddr, "provider_timeout", cfg.ProviderTimeout.String(),
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
