//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/platform/shutdown"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// R17: cancelling the worker while 50 sends are in flight leaves every row sent or queued (never
// stranded in sending), and a restart delivers the rest with exactly one provider call each.
func TestWorkerShutdownWithSendsInFlight(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: http.StatusAccepted, Delay: 500 * time.Millisecond}
	})
	cfg := defaultPipeline(mock.SendURL())
	cfg.concurrency = 50
	p := startPipeline(t, cfg)

	ids := make([]string, 50)
	for i := range ids {
		ids[i] = p.create(t, app.CreateRequest{}).ID
	}
	waitFor(t, 10*time.Second, "50 sends in flight", func() bool { return len(mock.Received()) >= 50 })
	p.stop() // root ctx cancelled mid-send, like SIGTERM

	for _, id := range ids {
		if st, _ := statusOf(t, id); st != "sent" && st != "queued" {
			t.Fatalf("%s ended %s after shutdown, want sent or queued", id, st)
		}
	}

	startPipeline(t, cfg)
	waitFor(t, 20*time.Second, "all sent after restart", func() bool {
		for _, id := range ids {
			if st, _ := statusOf(t, id); st != "sent" {
				return false
			}
		}
		return true
	})
	calls := map[string]int{}
	for _, r := range mock.Received() {
		calls[r.IdempotencyKey]++
	}
	for _, id := range ids {
		if calls[id] != 1 {
			t.Errorf("%s: %d provider calls, want exactly 1", id, calls[id])
		}
	}
}

// SC-004, US1-AS11: SIGTERM during 200 concurrent creates. Every request either got 202 with a
// durable row, or failed with no row stored.
func TestAPIShutdownDuringConcurrentCreates(t *testing.T) {
	env.Truncate(t)
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := httpapi.NewHTTPServer(addr, apiHandler(quietLog))
	ctx, sigterm := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- shutdown.Run(ctx, quietLog, 10*time.Second, shutdown.HTTPServer("http", srv, 10*time.Second))
	}()
	waitFor(t, 5*time.Second, "api listening", func() bool {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", addr)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	})

	const n = 200
	accepted := make([]bool, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			time.Sleep(time.Duration(i%50) * time.Millisecond) // spread across the shutdown
			body := fmt.Sprintf(`{"recipient":"+90555%07d","channel":"sms","content":"hi","idempotency_key":"sd-%d"}`, i, i)
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
				"http://"+addr+"/api/v1/notifications", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			_ = resp.Body.Close()
			accepted[i] = resp.StatusCode == http.StatusAccepted
		}()
	}
	close(start)
	time.Sleep(20 * time.Millisecond)
	sigterm()
	wg.Wait()
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	stored := map[string]bool{}
	rows, err := env.Pool.Query(context.Background(), `SELECT idempotency_key FROM notifications WHERE idempotency_key LIKE 'sd-%'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		stored[k] = true
	}
	rows.Close()

	got202 := 0
	for i, ok := range accepted {
		key := fmt.Sprintf("sd-%d", i)
		if ok {
			got202++
			if !stored[key] {
				t.Errorf("%s: 202 without a durable row", key)
			}
		} else if stored[key] {
			t.Errorf("%s: request failed but a row was stored", key)
		}
	}
	if got202 == 0 || got202 == n {
		t.Logf("note: %d of %d accepted; the shutdown didn't split the burst", got202, n)
	}
	t.Logf("%d accepted, %d refused after SIGTERM", got202, n-got202)
}
