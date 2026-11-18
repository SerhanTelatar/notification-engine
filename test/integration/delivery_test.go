//go:build integration

package integration

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

func setupDelivery(t *testing.T, script testenv.Script, mut func(*pipelineConfig)) (*pipeline, *testenv.MockProvider) {
	t.Helper()
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	if script != nil {
		mock.SetScript(script)
	}
	cfg := defaultPipeline(mock.SendURL())
	if mut != nil {
		mut(&cfg)
	}
	return startPipeline(t, cfg), mock
}

func statusIs(t *testing.T, id, want string) func() bool {
	return func() bool { st, _ := statusOf(t, id); return st == want }
}

func TestDeliverySendsOnce(t *testing.T) {
	p, mock := setupDelivery(t, nil, nil)
	n := p.create(t, app.CreateRequest{CorrelationID: "it-sent"})

	waitFor(t, 10*time.Second, "sent", statusIs(t, n.ID, "sent"))
	got := mock.Received()
	if len(got) != 1 || got[0].IdempotencyKey != n.ID || got[0].CorrelationID != "it-sent" || got[0].Content != "hello" {
		t.Fatalf("provider received %+v, want one call carrying Idempotency-Key and X-Correlation-ID", got)
	}
	var msgID *string
	_ = env.Pool.QueryRow(context.Background(), `SELECT provider_message_id FROM notifications WHERE id = $1`, n.ID).Scan(&msgID)
	if a := attemptsFor(t, n.ID); len(a) != 1 || a[0].outcome != "success" || msgID == nil {
		t.Fatalf("attempts %+v message id %v", a, msgID)
	}
}

func TestDeliveryRetriesThenSucceeds(t *testing.T) {
	p, mock := setupDelivery(t, func(n int, _ testenv.Received) testenv.Response {
		if n == 0 {
			return testenv.Response{Status: http.StatusServiceUnavailable}
		}
		return testenv.Response{Status: http.StatusAccepted}
	}, nil)
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 10*time.Second, "sent after a retry", statusIs(t, n.ID, "sent"))
	a := attemptsFor(t, n.ID)
	if len(a) != 2 || a[0].outcome != "transient" || *a[0].status != 503 || a[1].outcome != "success" || len(mock.Received()) != 2 {
		t.Fatalf("attempts %+v", a)
	}
	if _, att := statusOf(t, n.ID); att != 2 {
		t.Fatalf("attempts = %d, want 2", att)
	}
}

func TestDeliveryExhaustsRetriesIntoDLQ(t *testing.T) {
	p, mock := setupDelivery(t, func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: http.StatusServiceUnavailable}
	}, nil)
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 15*time.Second, "failed", statusIs(t, n.ID, "failed"))
	if a := attemptsFor(t, n.ID); len(a) != 5 || len(mock.Received()) != 5 {
		t.Fatalf("attempts %d, provider calls %d, want 5 and 5 (max_attempts counts the first)", len(a), len(mock.Received()))
	}
	waitFor(t, 5*time.Second, "one message in the DLQ", func() bool { return queueDepth(t, rabbitmq.DLQ) == 1 })
	if p.metrics.get("dead:exhausted") != 1 {
		t.Fatalf("dead-letter metric %d", p.metrics.get("dead:exhausted"))
	}
}

func TestDeliveryHonoursRetryAfter(t *testing.T) {
	p, mock := setupDelivery(t, func(n int, _ testenv.Received) testenv.Response {
		if n == 0 {
			return testenv.Response{Status: http.StatusTooManyRequests, RetryAfter: "2"}
		}
		return testenv.Response{Status: http.StatusAccepted}
	}, nil)
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 15*time.Second, "sent after Retry-After", statusIs(t, n.ID, "sent"))
	got := mock.Received()
	if len(got) != 2 {
		t.Fatalf("provider calls %d", len(got))
	}
	if gap := got[1].At.Sub(got[0].At); gap < 2*time.Second {
		t.Fatalf("second attempt after %v, want >= 2s (Retry-After floor over a 200ms backoff cap)", gap)
	}
}

func TestDeliveryPermanentFailureIsNotRetried(t *testing.T) {
	p, mock := setupDelivery(t, func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: http.StatusBadRequest}
	}, nil)
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 10*time.Second, "failed", statusIs(t, n.ID, "failed"))
	time.Sleep(300 * time.Millisecond) // longer than the backoff cap: no retry may follow
	if a := attemptsFor(t, n.ID); len(a) != 1 || a[0].outcome != "permanent" || len(mock.Received()) != 1 {
		t.Fatalf("attempts %+v calls %d", a, len(mock.Received()))
	}
	waitFor(t, 5*time.Second, "DLQ", func() bool { return queueDepth(t, rabbitmq.DLQ) == 1 })
}

func TestDeliveryTimeoutIsTransient(t *testing.T) {
	p, _ := setupDelivery(t, func(n int, _ testenv.Received) testenv.Response {
		if n == 0 {
			return testenv.Response{Status: http.StatusAccepted, Delay: 2 * time.Second}
		}
		return testenv.Response{Status: http.StatusAccepted}
	}, func(c *pipelineConfig) { c.providerTimeout = 300 * time.Millisecond })
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 10*time.Second, "sent after a timeout", statusIs(t, n.ID, "sent"))
	a := attemptsFor(t, n.ID)
	if len(a) != 2 || a[0].outcome != "transient" || a[0].status != nil || a[0].errText == nil {
		t.Fatalf("attempts %+v: the timed-out call must be recorded as transient without a status", a)
	}
}

func TestDeliveryAcceptsUnparsableSuccess(t *testing.T) {
	p, _ := setupDelivery(t, func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: http.StatusOK, Body: "<html>ok</html>"}
	}, nil)
	n := p.create(t, app.CreateRequest{})

	waitFor(t, 10*time.Second, "sent", statusIs(t, n.ID, "sent"))
	var msgID *string
	_ = env.Pool.QueryRow(context.Background(), `SELECT provider_message_id FROM notifications WHERE id = $1`, n.ID).Scan(&msgID)
	if msgID != nil {
		t.Fatalf("message id %q, want NULL for an unparsable body", *msgID)
	}
}

func TestDeliveryConcurrentSameKeySendsOnce(t *testing.T) {
	p, mock := setupDelivery(t, nil, nil)
	key := "sc-005"
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.create(t, app.CreateRequest{IdempotencyKey: &key})
		}()
	}
	wg.Wait()

	waitFor(t, 10*time.Second, "sent", func() bool { return countStatus(t, "sent") == 1 })
	time.Sleep(500 * time.Millisecond) // give any duplicate a chance to surface
	if got := len(mock.Received()); got != 1 || rowCount(t) != 1 {
		t.Fatalf("provider calls %d rows %d, want exactly 1 and 1 (SC-005)", got, rowCount(t))
	}
}
