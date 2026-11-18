// Package shutdown runs long-lived components under one signal-aware context and stops them
// gracefully: stop accepting work, drain in flight within a bound, then return (research R17).
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ErrTimeout means at least one component didn't stop within the shutdown timeout.
var ErrTimeout = errors.New("shutdown timed out")

// Component is a long-running unit (HTTP server, relay loop, consumer ...). Run must block until
// ctx is cancelled, finish its in-flight work, and then return.
type Component struct {
	Name string
	Run  func(ctx context.Context) error
}

// SignalContext is cancelled on SIGTERM or SIGINT.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGTERM, os.Interrupt)
}

// Run starts every component and blocks until ctx is cancelled or any component fails. It then
// cancels the rest and waits up to timeout for them to return. Context cancellation isn't treated
// as an error; component failures and a missed deadline are returned joined.
func Run(ctx context.Context, log *slog.Logger, timeout time.Duration, comps ...Component) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		name string
		err  error
	}
	done := make(chan result, len(comps))
	for _, c := range comps {
		go func() {
			err := c.Run(runCtx)
			done <- result{c.Name, err}
		}()
	}

	var errs []error
	remaining := len(comps)
	record := func(r result) {
		remaining--
		if r.err != nil && !errors.Is(r.err, context.Canceled) {
			log.ErrorContext(ctx, "component failed", "component", r.name, "error", r.err)
			errs = append(errs, fmt.Errorf("%s: %w", r.name, r.err))
		} else {
			log.InfoContext(ctx, "component stopped", "component", r.name)
		}
	}

	// Phase 1: run until a signal or the first component exits.
	select {
	case <-runCtx.Done():
	case r := <-done:
		record(r)
	}
	log.InfoContext(ctx, "shutting down", "timeout", timeout.String())
	cancel()

	// Phase 2: bounded drain.
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for remaining > 0 {
		select {
		case r := <-done:
			record(r)
		case <-deadline.C:
			errs = append(errs, fmt.Errorf("%w: %d component(s) still running", ErrTimeout, remaining))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// HTTPServer wraps srv as a Component. When ctx is cancelled it stops accepting connections and
// waits up to drain for in-flight requests (http.Server.Shutdown).
func HTTPServer(name string, srv *http.Server, drain time.Duration) Component {
	return Component{Name: name, Run: func(ctx context.Context) error {
		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", srv.Addr, err)
		}
		return serve(ctx, srv, ln, drain)
	}}
}

func serve(ctx context.Context, srv *http.Server, ln net.Listener, drain time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drain)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		return nil
	}
}
