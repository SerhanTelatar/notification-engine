// Command mockprovider imitates the external notification provider for demos and failure drills
// (contracts/provider.md). It isn't part of the product.
//
//	POST   /send       provider endpoint: 202 {"messageId","status":"accepted","timestamp"}
//	PUT    /control    change behaviour, optionally for one channel
//	GET    /control    current behaviour
//	DELETE /control    back to the env defaults
//	GET    /received   requests received (with timestamps), ?channel= to filter
//	DELETE /received   forget them
//
// Env defaults: MOCK_ADDR (:8090), MOCK_FAILURE_RATE (0), MOCK_LATENCY (20ms), MOCK_RETRY_AFTER (1).
// Per-request overrides for curl experiments: X-Mock-Status, X-Mock-Failure-Rate, X-Mock-Latency.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const maxRecorded = 200_000

// behaviour is what /send does; a zero status means "decide by failure rate".
type behaviour struct {
	FailureRate float64       `json:"failure_rate"`
	Status      int           `json:"status,omitempty"`
	Latency     time.Duration `json:"-"`
	LatencyText string        `json:"latency"`
}

type controlRequest struct {
	FailureRate *float64 `json:"failure_rate"`
	Status      *int     `json:"status"`
	Latency     *string  `json:"latency"`
	Channel     string   `json:"channel"` // empty = every channel
}

type received struct {
	At             time.Time `json:"at"`
	To             string    `json:"to"`
	Channel        string    `json:"channel"`
	Content        string    `json:"content"`
	IdempotencyKey string    `json:"idempotency_key"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	Status         int       `json:"status"`
}

type mock struct {
	log        *slog.Logger
	retryAfter string

	mu        sync.Mutex
	defaults  behaviour
	global    behaviour
	perChan   map[string]behaviour
	received  []received
	messageNo int
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockprovider")
	m := &mock{log: log, retryAfter: env("MOCK_RETRY_AFTER", "1"), perChan: map[string]behaviour{}}
	rate, _ := strconv.ParseFloat(env("MOCK_FAILURE_RATE", "0"), 64)
	lat, err := time.ParseDuration(env("MOCK_LATENCY", "20ms"))
	if err != nil {
		lat = 20 * time.Millisecond
	}
	m.defaults = behaviour{FailureRate: rate, Latency: lat, LatencyText: lat.String()}
	m.global = m.defaults

	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", m.send)
	mux.HandleFunc("PUT /control", m.putControl)
	mux.HandleFunc("GET /control", m.getControl)
	mux.HandleFunc("DELETE /control", m.resetControl)
	mux.HandleFunc("GET /received", m.listReceived)
	mux.HandleFunc("DELETE /received", m.clearReceived)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })

	srv := &http.Server{Addr: env("MOCK_ADDR", ":8090"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("mock provider listening", "addr", srv.Addr, "failure_rate", rate, "latency", lat.String())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func (m *mock) send(w http.ResponseWriter, r *http.Request) {
	arrived := time.Now().UTC() // before any simulated latency: this is when the request arrived
	var body struct{ To, Channel, Content string }
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	b := m.behaviourFor(body.Channel, r.Header)
	if b.Latency > 0 {
		select {
		case <-time.After(b.Latency):
		case <-r.Context().Done():
			return // the caller timed out
		}
	}
	status := b.Status
	if status == 0 {
		status = http.StatusAccepted
		if b.FailureRate > 0 && rand.Float64() < b.FailureRate { //nolint:gosec // simulation, not security
			status = http.StatusServiceUnavailable
		}
	}

	m.mu.Lock()
	if len(m.received) < maxRecorded {
		m.received = append(m.received, received{At: arrived, To: body.To, Channel: body.Channel,
			Content: body.Content, IdempotencyKey: r.Header.Get("Idempotency-Key"),
			CorrelationID: r.Header.Get("X-Correlation-ID"), Status: status})
	}
	m.messageNo++
	id := fmt.Sprintf("mock-%d", m.messageNo)
	m.mu.Unlock()

	switch {
	case status >= 200 && status < 300:
		writeJSON(w, status, map[string]string{"messageId": id, "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339)})
	case status == http.StatusTooManyRequests:
		w.Header().Set("Retry-After", m.retryAfter)
		writeJSON(w, status, map[string]string{"error": "rate limited"})
	default:
		writeJSON(w, status, map[string]string{"error": fmt.Sprintf("mock failure %d", status)})
	}
}

// behaviourFor resolves per-request headers > per-channel control > global control.
func (m *mock) behaviourFor(channel string, h http.Header) behaviour {
	m.mu.Lock()
	b, ok := m.perChan[channel]
	if !ok {
		b = m.global
	}
	m.mu.Unlock()
	if v := h.Get("X-Mock-Status"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			b.Status = s
		}
	}
	if v := h.Get("X-Mock-Failure-Rate"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			b.FailureRate = f
		}
	}
	if v := h.Get("X-Mock-Latency"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			b.Latency = d
		}
	}
	return b
}

func (m *mock) putControl(w http.ResponseWriter, r *http.Request) {
	var c controlRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&c); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.global
	if c.Channel != "" {
		if pc, ok := m.perChan[c.Channel]; ok {
			b = pc
		}
	}
	if c.FailureRate != nil {
		b.FailureRate = *c.FailureRate
	}
	if c.Status != nil {
		b.Status = *c.Status // 0 clears a forced status
	}
	if c.Latency != nil {
		d, err := time.ParseDuration(*c.Latency)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "latency must be a duration like 500ms"})
			return
		}
		b.Latency, b.LatencyText = d, d.String()
	}
	if c.Channel == "" {
		m.global = b
	} else {
		m.perChan[c.Channel] = b
	}
	m.log.Info("behaviour changed", "channel", c.Channel, "failure_rate", b.FailureRate, "status", b.Status, "latency", b.LatencyText)
	writeJSON(w, http.StatusOK, m.snapshotLocked())
}

func (m *mock) getControl(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	writeJSON(w, http.StatusOK, m.snapshotLocked())
}

func (m *mock) resetControl(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.global, m.perChan = m.defaults, map[string]behaviour{}
	writeJSON(w, http.StatusOK, m.snapshotLocked())
}

func (m *mock) snapshotLocked() map[string]any {
	per := map[string]behaviour{}
	for k, v := range m.perChan {
		per[k] = v
	}
	return map[string]any{"global": m.global, "channels": per}
}

func (m *mock) listReceived(w http.ResponseWriter, r *http.Request) {
	ch := r.URL.Query().Get("channel")
	m.mu.Lock()
	out := make([]received, 0, len(m.received))
	for _, rec := range m.received {
		if ch == "" || rec.Channel == ch {
			out = append(out, rec)
		}
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "items": out})
}

func (m *mock) clearReceived(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	m.received = nil
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
