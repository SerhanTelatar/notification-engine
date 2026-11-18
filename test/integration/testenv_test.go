//go:build integration

package integration

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) { testenv.Main(m, &env) }

// TestEnvironment checks that the harness itself works: every dependency is reachable, the schema
// is migrated, and the mock provider scripts and records.
func TestEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var tables int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('notifications','notification_attempts','batches')`).Scan(&tables); err != nil || tables != 3 {
		t.Fatalf("migrated tables = %d, err = %v", tables, err)
	}
	env.Truncate(t)

	conn, err := amqp.Dial(env.AMQPURL)
	if err != nil {
		t.Fatalf("rabbitmq: %v", err)
	}
	_ = conn.Close()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}

	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(n int, _ testenv.Received) testenv.Response {
		if n == 0 {
			return testenv.Response{Status: http.StatusServiceUnavailable}
		}
		return testenv.Response{Status: http.StatusAccepted}
	})
	codes := []int{}
	for range 2 {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, mock.SendURL(),
			bytes.NewBufferString(`{"to":"+905551234567","channel":"sms","content":"hi"}`))
		req.Header.Set("Idempotency-Key", "n-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		codes = append(codes, resp.StatusCode)
	}
	got := mock.Received()
	if len(got) != 2 || codes[0] != 503 || codes[1] != 202 || got[0].To != "+905551234567" || got[1].IdempotencyKey != "n-1" {
		t.Fatalf("mock provider: codes=%v received=%+v", codes, got)
	}
}
