// Command api serves the HTTP API and runs database migrations.
//
//	api [serve]                 run the HTTP API (default)
//	api migrate up|down|status  manage the schema
//
// All dependencies are constructed here and injected explicitly; there's no global state
// (Constitution II).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/SerhanTelatar/notification-engine/api"
	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	redisadapter "github.com/SerhanTelatar/notification-engine/internal/adapters/redis"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/ws"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/platform/config"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
	"github.com/SerhanTelatar/notification-engine/internal/platform/shutdown"
	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.LoadAPI(config.FromOS())
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := shutdown.SignalContext(ctx)
	defer stop()

	sub := "serve"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "serve":
		return serve(ctx, log, cfg)
	case "migrate":
		return runMigrate(ctx, log, cfg.DatabaseURL, args[1:])
	default:
		return fmt.Errorf("unknown command %q (want serve or migrate)", sub)
	}
}

func serve(ctx context.Context, log *slog.Logger, cfg config.API) error {
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, "notification-engine-api")
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close() // after the HTTP server has drained

	reg := metrics.NewRegistry()
	apiMetrics := metrics.NewAPI(reg)
	tr, err := tracing.New(ctx, cfg.OTLPEndpoint, "notification-engine-api") // no-op without an endpoint
	if err != nil {
		return err
	}
	defer flushTraces(ctx, log, tr)

	// Redis carries live status events only (US9); without it the API stays ready but degraded.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: cfg.RedisAddr, DialTimeout: 2 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	defer func() { _ = rdb.Close() }()
	checker := health.ForAPI(health.DefaultTimeout, postgres.Ping(pool), redisadapter.Ping(rdb))

	repo := postgres.NewNotificationRepo(pool)
	templates := postgres.NewTemplateRepo(pool)
	// Status events go out through Redis after each commit and come back to every API instance's
	// WebSocket hub (US9, ADR 0007). Best-effort: Redis being down never fails a request.
	events := redisadapter.NewStatusBus(rdb, log.With("component", "status"))
	hub := ws.NewHub(ws.DefaultBuffer)

	creator := app.NewCreator(app.CreatorDeps{
		Repo: repo, Templates: templates, IDs: noop.UUIDv7{}, Clock: noop.Clock{}, Log: log, MaxAttempts: cfg.MaxAttempts,
		Events: events,
	})
	query := app.NewQuery(app.QueryDeps{Repo: repo})
	batcher := app.NewBatcher(app.BatcherDeps{Repo: postgres.NewBatchRepo(pool), Templates: templates, IDs: noop.UUIDv7{},
		Clock: noop.Clock{}, Events: events, Log: log, MaxAttempts: cfg.MaxAttempts})

	router := httpapi.NewRouter(log, httpapi.NewServer(httpapi.Deps{Log: log, Health: checker, Creator: creator,
		Query:     query,
		Templates: app.NewTemplates(app.TemplatesDeps{Repo: templates, IDs: noop.UUIDv7{}, Clock: noop.Clock{}, Log: log}),
		Summary:   app.NewSummary(app.SummaryDeps{Repo: postgres.NewSummaryRepo(pool), Clock: noop.Clock{}}),
		Batcher:   batcher,
		Canceller: app.NewCanceller(app.CancellerDeps{Repo: repo, Batches: postgres.NewBatchRepo(pool),
			Events: events, Clock: noop.Clock{}, Log: log}),
		Metrics: apiMetrics}), httpapi.WithMetrics(apiMetrics), httpapi.WithTracing(tr))
	httpapi.MountOps(router, reg, api.Spec)
	httpapi.MountWS(router, httpapi.WSDeps{Hub: hub, Query: query, Batches: batcher, Metrics: apiMetrics,
		Log: log.With("component", "ws")})
	srv := httpapi.NewHTTPServer(cfg.HTTPAddr, router)

	// The subscriber feeds the hub; when it stops (shutdown), every client is closed with 1001.
	statusFeed := shutdown.Component{Name: "status-subscriber", Run: func(ctx context.Context) error {
		defer hub.Shutdown()
		return events.Run(ctx, hub.Publish)
	}}

	log.InfoContext(ctx, "api starting", "addr", cfg.HTTPAddr, "max_attempts", cfg.MaxAttempts)
	err = shutdown.Run(ctx, log, cfg.ShutdownTimeout,
		shutdown.HTTPServer("http", srv, cfg.ShutdownTimeout),
		statusFeed,
	)
	log.InfoContext(context.WithoutCancel(ctx), "api stopped")
	return err
}

// flushTraces exports any buffered spans before the process exits.
// It runs after ctx is cancelled, so it detaches from it.
func flushTraces(ctx context.Context, log *slog.Logger, tr *tracing.Tracing) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tr.Shutdown(ctx); err != nil {
		log.WarnContext(ctx, "flushing traces failed", "error", err)
	}
}
