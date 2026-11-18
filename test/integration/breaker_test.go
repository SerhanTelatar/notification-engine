//go:build integration

package integration

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/provider"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// stateLog records breaker state changes per channel (0 closed, 1 half-open, 2 open).
type stateLog struct {
	mu     sync.Mutex
	states map[domain.Channel][]int
}

func (s *stateLog) record(ch domain.Channel, state int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = map[domain.Channel][]int{}
	}
	s.states[ch] = append(s.states[ch], state)
}

func (s *stateLog) last(ch domain.Channel) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.states[ch]; len(v) > 0 {
		return v[len(v)-1]
	}
	return 0
}

func (s *stateLog) everOpen(ch domain.Channel) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.states[ch] {
		if v == 2 {
			return true
		}
	}
	return false
}

func smsAttempts(t testing.TB) int {
	t.Helper()
	var n int
	_ = env.Pool.QueryRow(context.Background(), `SELECT coalesce(sum(attempts), 0) FROM notifications WHERE channel = 'sms'`).Scan(&n)
	return n
}

func sentFor(t testing.TB, channel string) int {
	t.Helper()
	var n int
	_ = env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE channel = $1 AND status = 'sent'`, channel).Scan(&n)
	return n
}

func TestBreakerOpensIsolatesTheChannelAndRecovers(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	var smsDown atomic.Bool
	smsDown.Store(true)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(_ int, r testenv.Received) testenv.Response {
		if r.Channel == "sms" && smsDown.Load() {
			return testenv.Response{Status: http.StatusServiceUnavailable}
		}
		return testenv.Response{Status: http.StatusAccepted}
	})
	states := &stateLog{}
	cfg := defaultPipeline(mock.SendURL())
	cfg.breaker = provider.NewBreakers(5, 2*time.Second, states.record)
	cfg.maxDeferAge = time.Hour
	p := startPipeline(t, cfg)

	for range 10 {
		p.create(t, app.CreateRequest{})
		p.create(t, app.CreateRequest{Channel: "email", Recipient: "ada@example.com"})
	}
	waitFor(t, 10*time.Second, "sms breaker open", func() bool { return states.last(domain.ChannelSMS) == 2 })

	// Open: no sms attempts are consumed (deferrals only) during less than one cool-down.
	frozen := smsAttempts(t)
	time.Sleep(time.Second)
	if now := smsAttempts(t); now != frozen {
		t.Fatalf("sms attempts grew from %d to %d while the breaker was open (FR-024)", frozen, now)
	}
	// Other channels keep flowing (SC-009).
	waitFor(t, 10*time.Second, "all email sent", func() bool { return sentFor(t, "email") == 10 })
	if states.everOpen(domain.ChannelEmail) {
		t.Fatal("the email breaker must not trip on sms failures")
	}

	// Provider recovers: sms resumes within about one cool-down.
	smsDown.Store(false)
	waitFor(t, 15*time.Second, "all sms sent after recovery", func() bool { return sentFor(t, "sms") == 10 })
	if states.last(domain.ChannelSMS) != 0 {
		t.Fatalf("sms breaker state %d after recovery, want closed", states.last(domain.ChannelSMS))
	}
}

func TestBreakerIgnoresPermanentFailures(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(int, testenv.Received) testenv.Response { return testenv.Response{Status: http.StatusBadRequest} })
	states := &stateLog{}
	cfg := defaultPipeline(mock.SendURL())
	cfg.breaker = provider.NewBreakers(5, 2*time.Second, states.record)
	p := startPipeline(t, cfg)
	for range 10 {
		p.create(t, app.CreateRequest{})
	}
	waitFor(t, 10*time.Second, "all failed permanently", func() bool { return countStatus(t, "failed") == 10 })
	if states.everOpen(domain.ChannelSMS) {
		t.Fatal("permanent 4xx failures must never trip the breaker (a bad recipient isn't an outage)")
	}
}

func TestBreakerDeferralPastMaxAgeFailsAsProviderUnavailable(t *testing.T) {
	// Analysis finding D1: the deferral loop is capped by BREAKER_MAX_DEFER_AGE.
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: http.StatusServiceUnavailable}
	})
	states := &stateLog{}
	cfg := defaultPipeline(mock.SendURL())
	cfg.breaker = provider.NewBreakers(5, 30*time.Second, states.record) // no trial during the test
	cfg.maxDeferAge = 3 * time.Second
	p := startPipeline(t, cfg)

	for range 5 { // trip the breaker
		p.create(t, app.CreateRequest{})
	}
	waitFor(t, 10*time.Second, "sms breaker open", func() bool { return states.last(domain.ChannelSMS) == 2 })
	n := p.create(t, app.CreateRequest{CorrelationID: "it-defer"})

	waitFor(t, 15*time.Second, "deferral cap reached", func() bool { st, _ := statusOf(t, n.ID); return st == "failed" })
	var lastErr string
	var attempts int
	_ = env.Pool.QueryRow(context.Background(), `SELECT coalesce(last_error, ''), attempts FROM notifications WHERE id = $1`, n.ID).Scan(&lastErr, &attempts)
	if lastErr != "provider_unavailable" || attempts != 0 {
		t.Fatalf("last_error %q attempts %d, want provider_unavailable after deferrals only", lastErr, attempts)
	}
	for _, r := range mock.Received() {
		if r.IdempotencyKey == n.ID {
			t.Fatal("a notification deferred by an open breaker must never reach the provider")
		}
	}
	waitFor(t, 5*time.Second, "dead-lettered", func() bool { return queueDepth(t, rabbitmq.DLQ) >= 1 })
}
