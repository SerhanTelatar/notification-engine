// Package config loads process configuration from environment variables (Constitution VIII).
// Defaults match .env.example and research R19; every problem is reported at once on startup.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Lookup reads one variable. os.LookupEnv in production, a map in tests.
type Lookup func(key string) (string, bool)

// Shared holds settings common to every binary.
type Shared struct {
	DatabaseURL     string
	RedisAddr       string
	LogLevel        string
	ShutdownTimeout time.Duration
	OTLPEndpoint    string // empty disables tracing
}

// API configures cmd/api.
type API struct {
	Shared
	HTTPAddr    string
	MaxAttempts int
}

// Worker configures cmd/worker.
type Worker struct {
	Shared
	OpsAddr string
	AMQPURL string

	ProviderURL     string
	ProviderTimeout time.Duration

	RateLimitPerSecond  int
	RateLimitSendOffset time.Duration
	RateLimitHeadroom   float64
	LimiterMaxWait      time.Duration
	BackoffBase         time.Duration
	BackoffMax          time.Duration

	BreakerConsecutiveFailures int
	BreakerCooldown            time.Duration
	BreakerMaxDeferAge         time.Duration

	RelayInterval              time.Duration
	RelayBatchSize             int
	RelayMaxInflightPerChannel int

	ConsumerConcurrencyPerChannel int

	ReaperInterval       time.Duration
	ReaperSendingTimeout time.Duration
	ReaperQueuedTimeout  time.Duration
}

// FromOS is the production Lookup.
func FromOS() Lookup { return os.LookupEnv }

// LoadAPI reads and validates the API configuration.
func LoadAPI(env Lookup) (API, error) {
	r := reader{env: env}
	c := API{
		Shared:      r.shared(),
		HTTPAddr:    r.str("HTTP_ADDR", ":8080"),
		MaxAttempts: r.integer("MAX_ATTEMPTS", 5, 1, 100),
	}
	r.addr("HTTP_ADDR", c.HTTPAddr)
	return c, r.err()
}

// LoadWorker reads and validates the worker configuration.
func LoadWorker(env Lookup) (Worker, error) {
	r := reader{env: env}
	c := Worker{
		Shared:  r.shared(),
		OpsAddr: r.str("OPS_ADDR", ":9090"),
		AMQPURL: r.str("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/"),

		ProviderURL:     r.str("PROVIDER_URL", "http://mockprovider:8090/send"),
		ProviderTimeout: r.duration("PROVIDER_TIMEOUT", 5*time.Second),

		RateLimitPerSecond:  r.integer("RATE_LIMIT_PER_SECOND", 100, 1, 100000),
		RateLimitSendOffset: r.duration("RATE_LIMIT_SEND_OFFSET", 50*time.Millisecond),
		RateLimitHeadroom:   r.fraction("RATE_LIMIT_HEADROOM", 0.05, 0, 0.5),
		LimiterMaxWait:      r.duration("LIMITER_MAX_WAIT", 30*time.Second),
		BackoffBase:         r.duration("BACKOFF_BASE", time.Second),
		BackoffMax:          r.duration("BACKOFF_MAX", 5*time.Minute),

		BreakerConsecutiveFailures: r.integer("BREAKER_CONSECUTIVE_FAILURES", 5, 1, 1000),
		BreakerCooldown:            r.duration("BREAKER_COOLDOWN", 30*time.Second),
		BreakerMaxDeferAge:         r.duration("BREAKER_MAX_DEFER_AGE", time.Hour),

		RelayInterval:              r.duration("RELAY_INTERVAL", 200*time.Millisecond),
		RelayBatchSize:             r.integer("RELAY_BATCH_SIZE", 500, 1, 10000),
		RelayMaxInflightPerChannel: r.integer("RELAY_MAX_INFLIGHT_PER_CHANNEL", 1000, 1, 1000000),

		ConsumerConcurrencyPerChannel: r.integer("CONSUMER_CONCURRENCY_PER_CHANNEL", 20, 1, 1000),

		ReaperInterval:       r.duration("REAPER_INTERVAL", 30*time.Second),
		ReaperSendingTimeout: r.duration("REAPER_SENDING_TIMEOUT", 2*time.Minute),
		ReaperQueuedTimeout:  r.duration("REAPER_QUEUED_TIMEOUT", 15*time.Minute),
	}
	r.addr("OPS_ADDR", c.OpsAddr)
	r.url("AMQP_URL", c.AMQPURL, "amqp", "amqps")
	r.url("PROVIDER_URL", c.ProviderURL, "http", "https")
	if c.BackoffMax < c.BackoffBase {
		r.fail("BACKOFF_MAX must be >= BACKOFF_BASE")
	}
	if c.ReaperSendingTimeout <= c.ProviderTimeout {
		r.fail("REAPER_SENDING_TIMEOUT must exceed PROVIDER_TIMEOUT, or in-flight sends would be reaped")
	}
	return c, r.err()
}

// reader accumulates validation errors so startup reports all of them at once.
type reader struct {
	env  Lookup
	errs []error
}

func (r *reader) shared() Shared {
	s := Shared{
		DatabaseURL:     r.str("DATABASE_URL", "postgres://notify:notify@postgres:5432/notify?sslmode=disable"),
		RedisAddr:       r.str("REDIS_ADDR", "redis:6379"),
		LogLevel:        strings.ToLower(r.str("LOG_LEVEL", "info")),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
		OTLPEndpoint:    r.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
	r.url("DATABASE_URL", s.DatabaseURL, "postgres", "postgresql")
	switch s.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		r.fail("LOG_LEVEL must be one of debug, info, warn, error")
	}
	return s
}

func (r *reader) fail(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf(format, args...))
}

func (r *reader) err() error {
	if len(r.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(r.errs...))
}

func (r *reader) raw(key string) (string, bool) {
	v, ok := r.env(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (r *reader) str(key, def string) string {
	if v, ok := r.raw(key); ok {
		return v
	}
	return def
}

func (r *reader) integer(key string, def, minV, maxV int) int {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail("%s: %q is not an integer", key, v)
		return def
	}
	if n < minV || n > maxV {
		r.fail("%s: %d is outside [%d, %d]", key, n, minV, maxV)
		return def
	}
	return n
}

func (r *reader) fraction(key string, def, minV, maxV float64) float64 {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < minV || f > maxV {
		r.fail("%s: %q must be a number in [%g, %g]", key, v, minV, maxV)
		return def
	}
	return f
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail("%s: %q is not a duration (e.g. 500ms, 5s, 1h)", key, v)
		return def
	}
	if d <= 0 {
		r.fail("%s: must be positive", key)
		return def
	}
	return d
}

func (r *reader) url(key, v string, schemes ...string) {
	u, err := url.Parse(v)
	if err != nil || u.Host == "" {
		r.fail("%s: %q is not a valid URL", key, redact(v))
		return
	}
	for _, s := range schemes {
		if u.Scheme == s {
			return
		}
	}
	r.fail("%s: scheme %q not one of %v", key, u.Scheme, schemes)
}

func (r *reader) addr(key, v string) {
	i := strings.LastIndex(v, ":")
	if i < 0 {
		r.fail("%s: %q must be host:port", key, v)
		return
	}
	if p, err := strconv.Atoi(v[i+1:]); err != nil || p < 0 || p > 65535 {
		r.fail("%s: %q has an invalid port", key, v)
	}
}

// redact hides credentials if a URL is echoed in an error.
func redact(v string) string {
	if u, err := url.Parse(v); err == nil && u.User != nil {
		u.User = url.User("redacted")
		return u.String()
	}
	return v
}
