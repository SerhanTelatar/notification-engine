// Command worker runs the delivery side: outbox relay, per-channel consumers, reaper and
// backlog sampler (added with US1, US3 and US7), plus an ops server exposing /healthz,
// /readyz and /metrics (Constitution IV). All dependencies are injected here.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
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

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, "notification-engine-worker")
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close() // after every component has stopped

	reg := metrics.NewRegistry()
	_ = metrics.NewWorker(reg) // instrumented with the delivery pipeline (US1/US7)

	// RabbitMQ and Redis join the required checks when their adapters are wired (US1, US3).
	checker := health.New(health.DefaultTimeout).
		Require("postgres", postgres.Ping(pool))

	components := []shutdown.Component{
		shutdown.HTTPServer("ops", newOpsServer(cfg.OpsAddr, checker, reg), cfg.ShutdownTimeout),
		// relay, consumers, reaper and sampler are added by later tasks
	}

	log.InfoContext(ctx, "worker starting", "ops_addr", cfg.OpsAddr,
		"rate_limit_per_second", cfg.RateLimitPerSecond,
		"relay_max_inflight_per_channel", cfg.RelayMaxInflightPerChannel)
	err = shutdown.Run(ctx, log, cfg.ShutdownTimeout, components...)
	log.InfoContext(context.WithoutCancel(ctx), "worker stopped")
	return err
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
