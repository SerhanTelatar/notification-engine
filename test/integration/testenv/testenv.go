//go:build integration

// Package testenv starts the real dependencies once per test package with testcontainers-go
// (Constitution V): PostgreSQL 16 with migrations applied, RabbitMQ 3.13 and Redis 7. It also
// provides a scriptable mock provider.
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // goose uses database/sql
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/SerhanTelatar/notification-engine/migrations"
)

// Pinned images; they match docker-compose.yml.
const (
	PostgresImage = "postgres:16-alpine"
	RabbitImage   = "rabbitmq:3.13-management-alpine"
	RedisImage    = "redis:7-alpine"
)

// Env holds connection details of the running containers.
type Env struct {
	PostgresURL string
	AMQPURL     string
	RedisAddr   string
	Pool        *pgxpool.Pool

	containers []testcontainers.Container
}

// Start launches all three containers in parallel and applies the migrations.
func Start(ctx context.Context) (*Env, error) {
	env := &Env{}
	type started struct {
		name string
		c    testcontainers.Container
		err  error
	}
	ch := make(chan started, 3)

	go func() {
		c, err := tcpostgres.Run(ctx, PostgresImage,
			tcpostgres.WithDatabase("notify"), tcpostgres.WithUsername("notify"), tcpostgres.WithPassword("notify"),
			tcpostgres.BasicWaitStrategies())
		ch <- started{"postgres", c, err}
	}()
	go func() {
		c, err := tcrabbit.Run(ctx, RabbitImage)
		ch <- started{"rabbitmq", c, err}
	}()
	go func() {
		c, err := tcredis.Run(ctx, RedisImage)
		ch <- started{"redis", c, err}
	}()

	var firstErr error
	for range 3 {
		s := <-ch
		if s.c != nil {
			env.containers = append(env.containers, s.c)
		}
		if s.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("start %s: %w", s.name, s.err)
			}
			continue
		}
		var err error
		switch c := s.c.(type) {
		case *tcpostgres.PostgresContainer:
			env.PostgresURL, err = c.ConnectionString(ctx, "sslmode=disable")
		case *tcrabbit.RabbitMQContainer:
			env.AMQPURL, err = c.AmqpURL(ctx)
		case *tcredis.RedisContainer:
			env.RedisAddr, err = c.Endpoint(ctx, "")
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s endpoint: %w", s.name, err)
		}
	}
	if firstErr != nil {
		env.Stop(context.WithoutCancel(ctx))
		return nil, firstErr
	}

	if err := migrateUp(ctx, env.PostgresURL); err != nil {
		env.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	pool, err := pgxpool.New(ctx, env.PostgresURL)
	if err != nil {
		env.Stop(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("pool: %w", err)
	}
	env.Pool = pool
	return env, nil
}

// Stop closes the pool and terminates every container.
func (e *Env) Stop(ctx context.Context) {
	if e.Pool != nil {
		e.Pool.Close()
	}
	for _, c := range e.containers {
		_ = testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx))
	}
}

// Truncate empties every table between tests.
func (e *Env) Truncate(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.Pool.Exec(ctx, `TRUNCATE notification_attempts, notifications, batches RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// Main runs a package's tests with one shared Env. Use it from TestMain:
//
//	var env *testenv.Env
//	func TestMain(m *testing.M) { testenv.Main(m, &env) }
func Main(m *testing.M, out **Env) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	env, err := Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "testenv:", err)
		os.Exit(1)
	}
	*out = env
	code := m.Run()
	env.Stop(context.Background())
	os.Exit(code)
}

func migrateUp(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("goose: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
