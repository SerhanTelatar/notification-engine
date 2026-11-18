package shutdown

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func blockUntilDone(stopped *atomic.Int32) func(context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		stopped.Add(1)
		return ctx.Err()
	}
}

func TestRun(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		comps       func(*atomic.Int32) []Component
		cancelAfter time.Duration // 0 = never cancel the parent
		timeout     time.Duration
		wantErr     error
		wantStopped int32
	}{
		{
			name: "signal stops every component cleanly",
			comps: func(s *atomic.Int32) []Component {
				return []Component{{"a", blockUntilDone(s)}, {"b", blockUntilDone(s)}}
			},
			cancelAfter: 20 * time.Millisecond,
			timeout:     time.Second,
			wantStopped: 2,
		},
		{
			name: "a failing component cancels the others",
			comps: func(s *atomic.Int32) []Component {
				return []Component{
					{"ok", blockUntilDone(s)},
					{"bad", func(context.Context) error { return boom }},
				}
			},
			timeout:     time.Second,
			wantErr:     boom,
			wantStopped: 1,
		},
		{
			name: "a component that ignores ctx hits the deadline",
			comps: func(*atomic.Int32) []Component {
				return []Component{{"stuck", func(context.Context) error { time.Sleep(time.Second); return nil }}}
			},
			cancelAfter: 10 * time.Millisecond,
			timeout:     50 * time.Millisecond,
			wantErr:     ErrTimeout,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stopped atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelAfter > 0 {
				time.AfterFunc(tc.cancelAfter, cancel)
			}
			err := Run(ctx, quiet, tc.timeout, tc.comps(&stopped)...)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if got := stopped.Load(); got != tc.wantStopped {
				t.Fatalf("stopped = %d, want %d", got, tc.wantStopped)
			}
		})
	}
}

func TestHTTPServerDrainsInFlightRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(100 * time.Millisecond) // in flight when shutdown begins
		w.WriteHeader(http.StatusNoContent)
	})}

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx, srv, ln, time.Second) }()

	respCh := make(chan int, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+ln.Addr().String(), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			respCh <- -1
			return
		}
		_ = resp.Body.Close()
		respCh <- resp.StatusCode
	}()

	<-started
	cancel()
	if code := <-respCh; code != http.StatusNoContent {
		t.Fatalf("in-flight request got %d, want 204", code)
	}
	if err := <-served; err != nil {
		t.Fatalf("serve returned %v", err)
	}
}
