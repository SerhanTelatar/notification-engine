//go:build integration

package integration

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

func cancelURL(base, id string) string { return base + "/api/v1/notifications/" + id + "/cancel" }

func TestCancelQueuedNotificationIsNeverSent(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	api := newAPI(t)
	mock := testenv.NewMockProvider(t)

	created := postJSON(t, api.URL+"/api/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"stop me"}`, nil)
	id := created.body["id"].(string)

	// Relay it: the row is queued and its message waits in RabbitMQ, with no consumer yet.
	conn, err := rabbitmq.Dial(env.AMQPURL, "test-cancel-relay")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	pub := rabbitmq.NewPublisher(conn, 0)
	defer pub.Close()
	relay := app.NewRelay(app.RelayDeps{Repo: postgres.NewNotificationRepo(env.Pool), Publisher: pub, Events: noop.Broadcaster{},
		Metrics: newCountingMetrics(), Clock: noop.Clock{}, IDs: noop.UUIDv7{}, Log: quietLog, MaxInflight: 1000, BatchSize: 500})
	if err := relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st, _ := statusOf(t, id); st != "queued" || queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)) != 1 {
		t.Fatalf("setup: status %s, depth %d", st, queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)))
	}

	r := postJSON(t, cancelURL(api.URL, id), ``, nil)
	if r.code != http.StatusOK || r.body["status"] != "cancelled" {
		t.Fatalf("cancel: %d %v", r.code, r.body)
	}
	// Now let a worker consume the stale message.
	startPipeline(t, defaultPipeline(mock.SendURL()))
	waitFor(t, 10*time.Second, "the stale message consumed", func() bool {
		return queueDepth(t, rabbitmq.QueueName(domain.ChannelSMS)) == 0
	})
	time.Sleep(200 * time.Millisecond)
	if len(mock.Received()) != 0 {
		t.Fatal("a cancelled notification reached the provider (FR-014)")
	}
	if st, _ := statusOf(t, id); st != "cancelled" {
		t.Fatalf("status %s", st)
	}

	again := postJSON(t, cancelURL(api.URL, id), ``, nil)
	if again.code != http.StatusOK || again.body["status"] != "cancelled" {
		t.Fatalf("re-cancel must be idempotent: %d %v", again.code, again.body)
	}
}

func TestCancelRacesDeliveryAndExactlyOneWins(t *testing.T) {
	const n = 100
	env.Truncate(t)
	resetBroker(t)
	api := newAPI(t)
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(int, testenv.Received) testenv.Response {
		return testenv.Response{Status: 202, Delay: 20 * time.Millisecond}
	})
	insertBurst(t, domain.ChannelSMS, "+905551234567", n)
	rows, err := env.Pool.Query(context.Background(), `SELECT id::text FROM notifications`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	cfg := defaultPipeline(mock.SendURL())
	cfg.concurrency = 5 // about 250/s: the 100 rows drain in ~0.4 s, so cancels land mid-drain
	startPipeline(t, cfg)
	// Fire the cancels only once delivery is under way, so some lose to sends and some hit rows
	// that are being claimed right now.
	waitFor(t, 10*time.Second, "delivery under way", func() bool { return len(mock.Received()) >= 20 })
	codes := make([]int, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i%10) * 5 * time.Millisecond) // spread cancels across the drain
			codes[i] = postJSON(t, cancelURL(api.URL, id), ``, nil).code
		}()
	}
	wg.Wait()
	waitFor(t, 20*time.Second, "every notification terminal", func() bool {
		return countStatus(t, "sent")+countStatus(t, "cancelled") == n
	})
	time.Sleep(200 * time.Millisecond)

	sentTo := map[string]int{}
	for _, r := range mock.Received() {
		sentTo[r.IdempotencyKey]++
	}
	var cancelled, sent int
	for i, id := range ids {
		st, _ := statusOf(t, id)
		switch st {
		case "cancelled":
			cancelled++
			if sentTo[id] != 0 {
				t.Fatalf("%s is cancelled but reached the provider (SC-006)", id)
			}
			if codes[i] != http.StatusOK {
				t.Fatalf("%s is cancelled but its cancel returned %d", id, codes[i])
			}
		case "sent":
			sent++
			if sentTo[id] != 1 {
				t.Fatalf("%s is sent with %d provider calls", id, sentTo[id])
			}
			if codes[i] != http.StatusConflict {
				t.Fatalf("%s was sent but its cancel returned %d, want 409", id, codes[i])
			}
		default:
			t.Fatalf("%s ended %s", id, st)
		}
	}
	t.Logf("race: %d cancelled, %d sent, no cancelled notification delivered", cancelled, sent)
	if cancelled == 0 || sent < 20 {
		t.Fatalf("race not exercised (%d cancelled, %d sent): both outcomes must occur", cancelled, sent)
	}
}

func TestCancelConflictsAndNotFound(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	created := postJSON(t, api.URL+"/api/v1/notifications", `{"recipient":"ada@example.com","channel":"email","content":"done"}`, nil)
	id := created.body["id"].(string)
	if _, err := env.Pool.Exec(context.Background(), `UPDATE notifications SET status='sent', sent_at=now(), attempts=1 WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	r := postJSON(t, cancelURL(api.URL, id), ``, nil)
	errBody, _ := r.body["error"].(map[string]any)
	if r.code != http.StatusConflict || errBody["code"] != "not_cancellable" || errBody["current_status"] != "sent" {
		t.Fatalf("cancel sent: %d %v (US5-AS2)", r.code, r.body)
	}
	unknown := "0193a3b0-0000-7000-8000-000000000001"
	if r := postJSON(t, cancelURL(api.URL, unknown), ``, nil); r.code != http.StatusNotFound {
		t.Fatalf("unknown notification: %d", r.code)
	}
	if r := postJSON(t, api.URL+"/api/v1/batches/"+unknown+"/cancel", ``, nil); r.code != http.StatusNotFound {
		t.Fatalf("unknown batch: %d", r.code)
	}
}

func TestCancelBatchWithMixedStates(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	created := postJSON(t, api.URL+"/api/v1/notifications/batch", batchBody(5, nil), nil)
	batchID := created.body["batch_id"].(string)
	items := created.body["items"].([]any)
	sentID := items[0].(map[string]any)["notification_id"].(string)
	cancelledID := items[1].(map[string]any)["notification_id"].(string)
	if _, err := env.Pool.Exec(context.Background(), `UPDATE notifications SET status='sent', sent_at=now(), attempts=1 WHERE id=$1`, sentID); err != nil {
		t.Fatal(err)
	}
	if r := postJSON(t, cancelURL(api.URL, cancelledID), ``, nil); r.code != http.StatusOK {
		t.Fatalf("pre-cancel: %d", r.code)
	}

	r := postJSON(t, api.URL+"/api/v1/batches/"+batchID+"/cancel", ``, nil)
	if r.code != http.StatusOK || r.body["cancelled"] != float64(3) || r.body["already_cancelled"] != float64(1) ||
		r.body["not_cancellable"] != float64(1) || r.body["batch_id"] != batchID {
		t.Fatalf("batch cancel: %d %v, want cancelled 3, already 1, not cancellable 1 (US5-AS5)", r.code, r.body)
	}
	if countStatus(t, "cancelled") != 4 || countStatus(t, "sent") != 1 {
		t.Fatalf("cancelled %d sent %d", countStatus(t, "cancelled"), countStatus(t, "sent"))
	}
}
