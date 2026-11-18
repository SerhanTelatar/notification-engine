//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

func getJSON(t testing.TB, url string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestAPIGetNotificationWithHistory(t *testing.T) {
	p, _ := setupDelivery(t, func(n int, _ testenv.Received) testenv.Response {
		if n == 0 {
			return testenv.Response{Status: http.StatusServiceUnavailable, Body: `{"error":"overloaded"}`}
		}
		return testenv.Response{Status: http.StatusAccepted}
	}, nil)
	api := newAPI(t)
	n := p.create(t, app.CreateRequest{CorrelationID: "it-get"})
	waitFor(t, 10*time.Second, "sent after one retry", statusIs(t, n.ID, "sent"))

	code, body := getJSON(t, api.URL+"/api/v1/notifications/"+n.ID)
	if code != http.StatusOK {
		t.Fatalf("GET %d %v", code, body)
	}
	if body["status"] != "sent" || body["attempts"] != float64(2) || body["content"] != "hello" ||
		body["correlation_id"] != "it-get" || body["provider_message_id"] != "msg-2" {
		t.Fatalf("detail %v", body)
	}
	for _, ts := range []string{"created_at", "updated_at", "sent_at"} {
		if _, err := time.Parse(time.RFC3339Nano, body[ts].(string)); err != nil {
			t.Fatalf("%s = %v: %v", ts, body[ts], err)
		}
	}
	history, _ := body["attempt_history"].([]any)
	if len(history) != 2 {
		t.Fatalf("attempt_history %v", body["attempt_history"])
	}
	first, second := history[0].(map[string]any), history[1].(map[string]any)
	if first["attempt_no"] != float64(1) || first["outcome"] != "transient" || first["http_status"] != float64(503) {
		t.Fatalf("first attempt %v", first)
	}
	if errText, _ := first["error"].(string); errText == "" {
		t.Fatalf("first attempt must carry its error: %v", first)
	}
	if second["attempt_no"] != float64(2) || second["outcome"] != "success" || second["http_status"] != float64(202) {
		t.Fatalf("second attempt %v", second)
	}
}

func TestAPIGetNotificationErrors(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)

	code, body := getJSON(t, api.URL+"/api/v1/notifications/0193a3b0-0000-7000-8000-000000000001")
	if code != http.StatusNotFound || body["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown id: %d %v", code, body)
	}
	code, body = getJSON(t, api.URL+"/api/v1/notifications/not-a-uuid")
	if code != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "validation_failed" {
		t.Fatalf("malformed id: %d %v", code, body)
	}
}

func TestAPIGetFreshNotificationHasEmptyHistory(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	r := postJSON(t, api.URL+"/api/v1/notifications", `{"recipient":"ada@example.com","channel":"email","content":"hi"}`, nil)
	code, body := getJSON(t, api.URL+"/api/v1/notifications/"+r.body["id"].(string))
	if code != http.StatusOK || body["status"] != "pending" {
		t.Fatalf("%d %v", code, body)
	}
	if h, ok := body["attempt_history"].([]any); !ok || len(h) != 0 {
		t.Fatalf("attempt_history must be an empty array, got %v", body["attempt_history"])
	}
}
