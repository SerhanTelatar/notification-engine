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
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/platform/config"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
	"github.com/SerhanTelatar/notification-engine/internal/platform/shutdown"
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

	// Redis carries live status events only (US9); without it the API stays ready but degraded.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: cfg.RedisAddr, DialTimeout: 2 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	defer func() { _ = rdb.Close() }()
	checker := health.ForAPI(health.DefaultTimeout, postgres.Ping(pool), redisadapter.Ping(rdb))

	repo := postgres.NewNotificationRepo(pool)
	templates := postgres.NewTemplateRepo(pool)
	creator := app.NewCreator(app.CreatorDeps{
		Repo: repo, Templates: templates, IDs: noop.UUIDv7{}, Clock: noop.Clock{}, Log: log, MaxAttempts: cfg.MaxAttempts,
		Events: noop.Broadcaster{}, // Redis fan-out arrives with US9
	})

	router := httpapi.NewRouter(log, httpapi.NewServer(httpapi.Deps{Log: log, Health: checker, Creator: creator,
		Query:     app.NewQuery(app.QueryDeps{Repo: repo}),
		Templates: app.NewTemplates(app.TemplatesDeps{Repo: templates, IDs: noop.UUIDv7{}, Clock: noop.Clock{}, Log: log}),
		Batcher: app.NewBatcher(app.BatcherDeps{Repo: postgres.NewBatchRepo(pool), Templates: templates, IDs: noop.UUIDv7{}, Clock: noop.Clock{},
			Events: noop.Broadcaster{}, Log: log, MaxAttempts: cfg.MaxAttempts}),
		Canceller: app.NewCanceller(app.CancellerDeps{Repo: repo, Batches: postgres.NewBatchRepo(pool),
			Events: noop.Broadcaster{}, Clock: noop.Clock{}, Log: log}),
		Metrics: apiMetrics}), httpapi.WithMetrics(apiMetrics))
	httpapi.MountOps(router, reg, api.Spec)
	srv := httpapi.NewHTTPServer(cfg.HTTPAddr, router)

	log.InfoContext(ctx, "api starting", "addr", cfg.HTTPAddr, "max_attempts", cfg.MaxAttempts)
	err = shutdown.Run(ctx, log, cfg.ShutdownTimeout,
		shutdown.HTTPServer("http", srv, cfg.ShutdownTimeout),
	)
	log.InfoContext(context.WithoutCancel(ctx), "api stopped")
	return err
}
