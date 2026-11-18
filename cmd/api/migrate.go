package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx" for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/migrations"
)

// runMigrate applies, rolls back or reports goose migrations: `api migrate up|down|status`.
// A Postgres session advisory lock makes concurrent runs safe (research R14).
func runMigrate(ctx context.Context, log *slog.Logger, databaseURL string, args []string) error {
	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect database: %w", err)
	}

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("migration provider: %w", err)
	}

	switch cmd {
	case "up":
		results, err := p.Up(ctx)
		for _, r := range results {
			log.InfoContext(ctx, "migration applied", "version", r.Source.Version, "file", r.Source.Path, "duration", r.Duration.String())
		}
		if err != nil {
			return fmt.Errorf("migrate up: %w", err)
		}
		v, err := p.GetDBVersion(ctx)
		if err != nil {
			return fmt.Errorf("read db version: %w", err)
		}
		log.InfoContext(ctx, "database is up to date", "version", v, "applied", len(results))
	case "down":
		r, err := p.Down(ctx)
		if err != nil {
			if errors.Is(err, goose.ErrNoNextVersion) {
				log.InfoContext(ctx, "nothing to roll back")
				return nil
			}
			return fmt.Errorf("migrate down: %w", err)
		}
		log.InfoContext(ctx, "migration rolled back", "version", r.Source.Version, "file", r.Source.Path)
	case "status":
		statuses, err := p.Status(ctx)
		if err != nil {
			return fmt.Errorf("migrate status: %w", err)
		}
		for _, s := range statuses {
			log.InfoContext(ctx, "migration", "version", s.Source.Version, "file", s.Source.Path, "state", string(s.State))
		}
	default:
		return fmt.Errorf("unknown migrate command %q (want up, down or status)", cmd)
	}
	return nil
}

// newLogger is shared by every subcommand.
func newLogger(level string) *slog.Logger { return logger.New("api", level) }
