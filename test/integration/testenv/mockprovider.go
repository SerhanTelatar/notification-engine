//go:build integration

package testenv

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Received is one request the mock provider accepted for inspection.
type Received struct {
	At             time.Time
	To             string `json:"to"`
	Channel        string `json:"channel"`
	Content        string `json:"content"`
	IdempotencyKey string
	CorrelationID  string
}

// Response is what the mock returns for one request.
type Response struct {
	Status     int
	Body       string        // empty: a provider-shaped success body on 2xx
	Delay      time.Duration // simulates latency and timeouts
	RetryAfter string        // Retry-After header value, if set
}

// Script decides the response for the n-th request (0-based) and its content.
type Script func(n int, r Received) Response

// Accept is the default script: 202 with a provider-shaped body.
func Accept(int, Received) Response { return Response{Status: http.StatusAccepted} }

// MockProvider is an httptest server that records requests with timestamps (rate and order
// checks) and answers with a scriptable Response (contracts/provider.md).
type MockProvider struct {
	*httptest.Server
	mu       sync.Mutex
	script   Script
	received []Received
}

// NewMockProvider starts a mock provider that is closed when the test ends.
func NewMockProvider(t testing.TB) *MockProvider {
	t.Helper()
	m := &MockProvider{script: Accept}
	m.Server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.Close)
	return m
}

// URL of the send endpoint.
func (m *MockProvider) SendURL() string { return m.URL + "/send" }

// SetScript replaces the response script.
func (m *MockProvider) SetScript(s Script) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.script = s
}

// Received returns a copy of every request so far.
func (m *MockProvider) Received() []Received {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Received(nil), m.received...)
}

// Reset clears recorded requests.
func (m *MockProvider) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = nil
}

func (m *MockProvider) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var rec Received
	_ = json.Unmarshal(body, &rec)
	rec.At = time.Now()
	rec.IdempotencyKey = r.Header.Get("Idempotency-Key")
	rec.CorrelationID = r.Header.Get("X-Correlation-ID")

	m.mu.Lock()
	n := len(m.received)
	m.received = append(m.received, rec)
	script := m.script
	m.mu.Unlock()

	resp := script(n, rec)
	if resp.Delay > 0 {
		select {
		case <-time.After(resp.Delay):
		case <-r.Context().Done():
			return
		}
	}
	if resp.RetryAfter != "" {
		w.Header().Set("Retry-After", resp.RetryAfter)
	}
	if resp.Status == 0 {
		resp.Status = http.StatusAccepted
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Status)
	if resp.Body != "" {
		_, _ = io.WriteString(w, resp.Body)
		return
	}
	if resp.Status >= 200 && resp.Status < 300 {
		_, _ = fmt.Fprintf(w, `{"messageId":"msg-%s","status":"accepted","timestamp":%q}`,
			strconv.Itoa(n+1), time.Now().UTC().Format(time.RFC3339))
	} else {
		_, _ = fmt.Fprintf(w, `{"error":"mock status %d"}`, resp.Status)
	}
}
