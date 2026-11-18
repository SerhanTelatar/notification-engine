package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SupervisorConfig configures a Supervisor.
type SupervisorConfig struct {
	URL        string
	Name       string // connection name shown in the management UI
	Log        *slog.Logger
	MinBackoff time.Duration // first redial delay; 0 = 500 ms
	MaxBackoff time.Duration // redial delay cap; 0 = 15 s
}

// Session runs everything that needs one broker connection (publisher, consumers, the loops that
// publish). It must return once ctx is cancelled; it may return earlier when the connection fails.
type Session func(ctx context.Context, conn *amqp.Connection) error

// Supervisor keeps a worker alive across broker outages (US7-AS4). It dials, declares the
// topology and runs a Session on the connection. When the connection is lost it cancels the
// session, waits for it to finish, and redials with capped exponential backoff. Postgres stays the
// source of truth meanwhile, so nothing is lost: rows wait as pending or queued, and the reaper
// republishes queued rows whose messages were in flight.
type Supervisor struct {
	cfg     SupervisorConfig
	session Session
	current atomic.Pointer[amqp.Connection] // nil while disconnected
}

// NewSupervisor returns a Supervisor that runs session on each new connection.
func NewSupervisor(cfg SupervisorConfig, session Session) *Supervisor {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 15 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	return &Supervisor{cfg: cfg, session: session}
}

// Alive is a readiness check: it fails while no session has a live connection.
func (s *Supervisor) Alive() func(context.Context) error {
	return func(context.Context) error {
		conn := s.current.Load()
		if conn == nil || conn.IsClosed() {
			return errors.New("rabbitmq connection down")
		}
		return nil
	}
}

// Run blocks until ctx is cancelled, reconnecting as needed. It returns nil on cancellation.
func (s *Supervisor) Run(ctx context.Context) error {
	backoff := s.cfg.MinBackoff
	for ctx.Err() == nil {
		conn, err := s.connect()
		if err != nil {
			s.cfg.Log.WarnContext(ctx, "rabbitmq unavailable, retrying", "retry_in", backoff.String(), "error", err)
			if !sleep(ctx, backoff) {
				break
			}
			backoff = min(backoff*2, s.cfg.MaxBackoff)
			continue
		}
		backoff = s.cfg.MinBackoff
		s.runSession(ctx, conn)
	}
	return nil
}

func (s *Supervisor) connect() (*amqp.Connection, error) {
	conn, err := Dial(s.cfg.URL, s.cfg.Name)
	if err != nil {
		return nil, err
	}
	if err := DeclareTopology(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// runSession runs one session until the connection is lost, the session returns, or ctx ends.
func (s *Supervisor) runSession(ctx context.Context, conn *amqp.Connection) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case amqpErr := <-closed:
			if amqpErr != nil {
				s.cfg.Log.ErrorContext(ctx, "rabbitmq connection lost; stopping the session to reconnect", "error", amqpErr)
			}
			cancel()
		case <-sctx.Done():
		}
	}()

	s.current.Store(conn)
	s.cfg.Log.InfoContext(ctx, "rabbitmq session started")
	err := s.session(sctx, conn)
	s.current.Store(nil)
	_ = conn.Close()
	if ctx.Err() == nil {
		s.cfg.Log.WarnContext(ctx, "rabbitmq session ended; reconnecting", "error", err)
		sleep(ctx, s.cfg.MinBackoff)
	}
}

// sleep waits d or until ctx ends; it reports whether the full delay elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
