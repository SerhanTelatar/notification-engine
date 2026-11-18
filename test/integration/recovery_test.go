//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// prepareRecovery resets the database and broker and starts a mock provider. Seed rows after it
// and before recoveryPipeline, so the relay can't touch them half-written.
func prepareRecovery(t *testing.T) *testenv.MockProvider {
	t.Helper()
	env.Truncate(t)
	resetBroker(t)
	return testenv.NewMockProvider(t)
}

func recoveryPipeline(t *testing.T, mock *testenv.MockProvider, mut func(*pipelineConfig)) *pipeline {
	t.Helper()
	cfg := defaultPipeline(mock.SendURL())
	cfg.sendingTimeout = time.Second
	cfg.queuedTimeout = time.Second
	cfg.reaperInterval = 200 * time.Millisecond
	if mut != nil {
		mut(&cfg)
	}
	return startPipeline(t, cfg)
}

func TestRecoveryStaleSendingIsRetried(t *testing.T) {
	mock := prepareRecovery(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	// A worker died during the provider call: the row is sending, long ago.
	n := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, n.ID, domain.StatusSending, time.Minute)
	recoveryPipeline(t, mock, nil)
	waitFor(t, 10*time.Second, "sent after recovery", func() bool { st, _ := statusOf(t, n.ID); return st == "sent" })

	a := attemptsFor(t, n.ID)
	if len(a) != 2 || a[0].outcome != "abandoned" || a[1].outcome != "success" || len(mock.Received()) != 1 {
		t.Fatalf("attempts %+v provider calls %d: want [abandoned, success] and one new call", a, len(mock.Received()))
	}
	if _, att := statusOf(t, n.ID); att != 2 {
		t.Fatalf("attempts %d: the abandoned call counts (it may have reached the provider)", att)
	}
}

func TestRecoveryStaleQueuedIsRepublished(t *testing.T) {
	mock := prepareRecovery(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	// The row is queued but its message was lost.
	n := mustCreate(t, repo, newNotification(t, domain.ChannelEmail, nil))
	moveTo(t, n.ID, domain.StatusQueued, time.Minute)
	p := recoveryPipeline(t, mock, nil)
	waitFor(t, 10*time.Second, "sent after republish", func() bool { st, _ := statusOf(t, n.ID); return st == "sent" })
	if len(mock.Received()) != 1 || p.metrics.get("reaped:queued") < 1 {
		t.Fatalf("provider calls %d, republished %d", len(mock.Received()), p.metrics.get("reaped:queued"))
	}
}

func TestRecoveryWorkerRestartMidBurst(t *testing.T) {
	const total = 300
	slow := func(c *pipelineConfig) { c.concurrency = 5 }
	mock := prepareRecovery(t)
	p := recoveryPipeline(t, mock, slow)
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: 202, Delay: 20 * time.Millisecond}
	})
	insertBurst(t, domain.ChannelSMS, "+905551234567", total)

	waitFor(t, 30*time.Second, "part of the burst sent", func() bool { return countStatus(t, "sent") >= 50 })
	p.stop() // SIGTERM mid-burst: stop taking work, finish in-flight sends
	sentAtStop := countStatus(t, "sent")
	if inFlight := countStatus(t, "sending"); inFlight != 0 {
		t.Fatalf("%d rows left in sending after a graceful stop", inFlight)
	}
	if sentAtStop >= total {
		t.Fatal("the burst finished before the stop; the test didn't interrupt anything")
	}

	// Restart: everything accepted reaches a terminal state, nothing is lost or sent twice.
	cfg := defaultPipeline(mock.SendURL())
	startPipeline(t, cfg)
	waitFor(t, 60*time.Second, "the whole burst sent after restart", func() bool { return countStatus(t, "sent") == total })
	if got := len(mock.Received()); got != total {
		t.Fatalf("provider calls %d, want %d (sent %d before the stop)", got, total, sentAtStop)
	}
	t.Logf("stopped after %d of %d, finished after restart with %d provider calls", sentAtStop, total, len(mock.Received()))
}

func TestRecoveryDuplicateMessageForSentRow(t *testing.T) {
	mock := prepareRecovery(t)
	p := recoveryPipeline(t, mock, func(c *pipelineConfig) { c.queuedTimeout = time.Hour })
	n := p.create(t, app.CreateRequest{})
	waitFor(t, 10*time.Second, "sent", func() bool { st, _ := statusOf(t, n.ID); return st == "sent" })

	// Redeliver the same work message, as a broker redelivery or a reaper republish would.
	dup := ports.Message{NotificationID: n.ID, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		CorrelationID: "dup", EnqueuedAt: time.Now()}
	if err := p.pub.Publish(context.Background(), []ports.Message{dup, dup}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "duplicates consumed", func() bool {
		return queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)) == 0
	})
	time.Sleep(200 * time.Millisecond)
	if got := len(mock.Received()); got != 1 {
		t.Fatalf("provider calls %d: duplicate messages for a sent row must be acked and dropped (FR-026)", got)
	}
	if queueDepth(t, rabbitmq.DLQ) != 0 {
		t.Fatal("dropped duplicates are acked, not dead-lettered")
	}
}
