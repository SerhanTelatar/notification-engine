// Command api serves the HTTP API and runs database migrations.
//
//	api migrate up|down|status
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/SerhanTelatar/notification-engine/internal/platform/config"
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

	sub := "migrate"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "migrate":
		return runMigrate(ctx, log, cfg.DatabaseURL, args[min(1, len(args)):])
	default:
		return fmt.Errorf("unknown command %q (want migrate)", sub)
	}
}
