//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	goredis "github.com/redis/go-redis/v9"

	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	redisadapter "github.com/SerhanTelatar/notification-engine/internal/adapters/redis"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/ws"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// wsStack is a worker publishing status events through Redis and an API instance that
// subscribes to them and serves /ws, like the two processes in docker compose.
type wsStack struct {
	api *httptest.Server
	hub *ws.Hub
}

func startWSStack(t *testing.T) wsStack {
	t.Helper()
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)

	workerRedis := goredis.NewClient(&goredis.Options{Addr: env.RedisAddr})
	t.Cleanup(func() { _ = workerRedis.Close() })
	cfg := defaultPipeline(mock.SendURL())
	cfg.events = redisadapter.NewStatusBus(workerRedis, quietLog)
	startPipeline(t, cfg)

	apiRedis := goredis.NewClient(&goredis.Options{Addr: env.RedisAddr})
	t.Cleanup(func() { _ = apiRedis.Close() })
	hub := ws.NewHub(0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = redisadapter.NewStatusBus(apiRedis, quietLog).Run(ctx, hub.Publish) }()
	t.Cleanup(func() { cancel(); <-done; hub.Shutdown() })

	router := apiHandler(quietLog)
	repo := postgres.NewNotificationRepo(env.Pool)
	httpapi.MountWS(router, httpapi.WSDeps{Hub: hub, Log: quietLog,
		Query: app.NewQuery(app.QueryDeps{Repo: repo}),
		Batches: app.NewBatcher(app.BatcherDeps{Repo: postgres.NewBatchRepo(env.Pool), IDs: noop.UUIDv7{}, Clock: noop.Clock{},
			Events: noop.Broadcaster{}, Log: quietLog, MaxAttempts: 5}),
	})
	api := httptest.NewServer(router)
	t.Cleanup(api.Close)
	time.Sleep(100 * time.Millisecond) // let the Redis subscription settle
	return wsStack{api: api, hub: hub}
}

func (s wsStack) dial(t *testing.T, query string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.api.URL, "http")+"/ws?"+query, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial %s: %v", query, err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func readMsg(t *testing.T, c *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func inTwoSeconds() string {
	return time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339Nano)
}

// Bad subscriptions are rejected before the upgrade.
func TestWSRejectsBadSubscriptions(t *testing.T) {
	s := startWSStack(t)
	for query, want := range map[string]int{
		"": http.StatusBadRequest, // neither
		"notification_id=0190f4a0-0000-7000-8000-000000000001&batch_id=0190f4a0-0000-7000-8000-000000000002": http.StatusBadRequest, // both
		"notification_id=not-a-uuid":                           http.StatusBadRequest,
		"notification_id=0190f4a0-0000-7000-8000-000000000001": http.StatusNotFound,
		"batch_id=0190f4a0-0000-7000-8000-000000000001":        http.StatusNotFound,
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.api.URL, "http")+"/ws?"+query, nil)
		cancel()
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err == nil || resp == nil || resp.StatusCode != want {
			code := 0
			if resp != nil {
				code = resp.StatusCode
			}
			t.Fatalf("%q: status %d (err %v), want %d", query, code, err, want)
		}
	}
}

// SC-010: a subscriber gets the snapshot, then every transition the worker makes, each within
// 1 s of the commit.
func TestWSNotificationSubscriptionSeesWorkerTransitions(t *testing.T) {
	s := startWSStack(t)
	res := postJSON(t, s.api.URL+"/api/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"ws","scheduled_at":"`+inTwoSeconds()+`"}`, nil)
	id, _ := res.body["id"].(string)

	c := s.dial(t, "notification_id="+id)
	snap := readMsg(t, c, 2*time.Second)
	items, _ := snap["notifications"].([]any)
	if snap["type"] != "snapshot" || len(items) != 1 || items[0].(map[string]any)["status"] != "scheduled" ||
		items[0].(map[string]any)["notification_id"] != id {
		t.Fatalf("snapshot %v", snap)
	}

	var got []string
	for _, want := range []string{"queued", "sending", "sent"} {
		m := readMsg(t, c, 10*time.Second)
		received := time.Now()
		at, err := time.Parse(time.RFC3339Nano, m["at"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if m["type"] != "status" || m["notification_id"] != id || m["to"] != want {
			t.Fatalf("message %v, want status %s (so far %v)", m, want, got)
		}
		if lag := received.Sub(at); lag > time.Second {
			t.Fatalf("%s arrived %v after the commit, want ≤ 1 s (SC-010)", want, lag)
		}
		got = append(got, want)
	}
}

// A batch subscription gets the events of every item.
func TestWSBatchSubscriptionSeesEveryItem(t *testing.T) {
	s := startWSStack(t)
	at := inTwoSeconds()
	res := postJSON(t, s.api.URL+"/api/v1/notifications/batch", `{"items":[
		{"recipient":"+905551234561","channel":"sms","content":"a","scheduled_at":"`+at+`"},
		{"recipient":"ada@example.com","channel":"email","content":"b","scheduled_at":"`+at+`"},
		{"recipient":"device-1","channel":"push","content":"c","scheduled_at":"`+at+`"}]}`, nil)
	batchID, _ := res.body["batch_id"].(string)

	c := s.dial(t, "batch_id="+batchID)
	snap := readMsg(t, c, 2*time.Second)
	if items, _ := snap["notifications"].([]any); snap["type"] != "snapshot" || len(items) != 3 {
		t.Fatalf("snapshot %v", snap)
	}
	sent := map[string]bool{}
	for len(sent) < 3 {
		m := readMsg(t, c, 10*time.Second)
		if m["batch_id"] != batchID {
			t.Fatalf("event for another batch: %v", m)
		}
		if m["to"] == "sent" {
			sent[m["notification_id"].(string)] = true
		}
	}
}
