// Package postgres implements the persistence ports on PostgreSQL (pgx/v5 + sqlc). PostgreSQL
// is the single source of truth (Constitution III).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// Pool defaults. Every statement is bounded server-side as well as by ctx (Constitution III).
const (
	DefaultMaxConns         = 20
	DefaultConnectTimeout   = 5 * time.Second
	DefaultStatementTimeout = 10 * time.Second
)

// ErrUniqueViolation wraps a unique-constraint failure. errors.As into *UniqueViolationError
// gives the constraint name.
var ErrUniqueViolation = errors.New("unique constraint violated")

// UniqueViolationError names the violated constraint.
type UniqueViolationError struct{ Constraint string }

func (e *UniqueViolationError) Error() string {
	return fmt.Sprintf("%s: %s", ErrUniqueViolation, e.Constraint)
}

// Unwrap lets errors.Is(err, ErrUniqueViolation) match.
func (e *UniqueViolationError) Unwrap() error { return ErrUniqueViolation }

// NewPool connects with bounded connect and statement timeouts and verifies connectivity.
func NewPool(ctx context.Context, databaseURL, appName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns < DefaultMaxConns {
		cfg.MaxConns = DefaultMaxConns
	}
	cfg.ConnConfig.ConnectTimeout = DefaultConnectTimeout
	rp := cfg.ConnConfig.RuntimeParams
	if rp["statement_timeout"] == "" {
		rp["statement_timeout"] = fmt.Sprint(DefaultStatementTimeout.Milliseconds())
	}
	rp["application_name"] = appName

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, DefaultConnectTimeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Ping returns a readiness check for the pool.
func Ping(pool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("postgres ping: %w", err)
		}
		return nil
	}
}

// WithTx runs fn in a transaction: committed if fn returns nil, rolled back otherwise (including
// on panic, which is then re-raised).
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) (err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
			}
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// translate maps driver errors to domain/adapter sentinels and adds context with %w.
func translate(err error, op string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, domain.ErrNotFound)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%s: %w", op, &UniqueViolationError{Constraint: pgErr.ConstraintName})
	}
	return fmt.Errorf("%s: %w", op, err)
}
