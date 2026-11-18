//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/noop"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// newAPI serves the real HTTP adapter over the real repository (no worker).
func newAPI(t testing.TB) *httptest.Server {
	t.Helper()
	repo := postgres.NewNotificationRepo(env.Pool)
	creator := app.NewCreator(app.CreatorDeps{
		Repo: repo, IDs: noop.UUIDv7{}, Clock: noop.Clock{}, Events: noop.Broadcaster{}, Log: quietLog, MaxAttempts: 5,
	})
	srv := httpapi.NewServer(httpapi.Deps{Log: quietLog, Health: health.New(0), Creator: creator,
		Query: app.NewQuery(app.QueryDeps{Repo: repo}),
		Batcher: app.NewBatcher(app.BatcherDeps{Repo: postgres.NewBatchRepo(env.Pool), IDs: noop.UUIDv7{}, Clock: noop.Clock{},
			Events: noop.Broadcaster{}, Log: quietLog, MaxAttempts: 5}),
		Canceller: app.NewCanceller(app.CancellerDeps{Repo: repo, Batches: postgres.NewBatchRepo(env.Pool),
			Events: noop.Broadcaster{}, Clock: noop.Clock{}, Log: quietLog})})
	ts := httptest.NewServer(httpapi.NewRouter(quietLog, srv))
	t.Cleanup(ts.Close)
	return ts
}

type apiResponse struct {
	code   int
	header http.Header
	body   map[string]any
}

func postJSON(t testing.TB, url, body string, headers map[string]string) apiResponse {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := apiResponse{code: resp.StatusCode, header: resp.Header}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func errorFields(r apiResponse) []string {
	e, _ := r.body["error"].(map[string]any)
	details, _ := e["details"].([]any)
	var fields []string
	for _, d := range details {
		if m, ok := d.(map[string]any); ok {
			fields = append(fields, m["field"].(string))
		}
	}
	return fields
}

func errorCode(r apiResponse) string {
	e, _ := r.body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func rowCount(t testing.TB) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAPICreateAccepted(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	url := api.URL + "/api/v1/notifications"

	r := postJSON(t, url, `{"recipient":"+905551234567","channel":"sms","content":"Your code is 1234","priority":"high"}`,
		map[string]string{"X-Correlation-ID": "it-create-1"})
	if r.code != http.StatusAccepted || r.body["status"] != "pending" || r.body["priority"] != "high" {
		t.Fatalf("got %d %v", r.code, r.body)
	}
	if r.header.Get("X-Correlation-ID") != "it-create-1" || r.body["correlation_id"] != "it-create-1" {
		t.Fatalf("correlation id not echoed/stored: header=%q body=%v", r.header.Get("X-Correlation-ID"), r.body["correlation_id"])
	}
	if r.header.Get("Idempotent-Replayed") != "" {
		t.Fatal("a new notification is not a replay")
	}

	future := time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339)
	r = postJSON(t, url, `{"recipient":"ada@example.com","channel":"email","content":"hi","scheduled_at":"`+future+`"}`, nil)
	if r.code != http.StatusAccepted || r.body["status"] != "scheduled" || r.header.Get("X-Correlation-ID") == "" {
		t.Fatalf("scheduled: %d %v", r.code, r.body)
	}
	if rowCount(t) != 2 {
		t.Fatalf("rows = %d", rowCount(t))
	}
}

func TestAPICreateValidation(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	url := api.URL + "/api/v1/notifications"
	tests := []struct {
		name       string
		body       string
		wantFields []string
	}{
		{"sms bad recipient and 161 chars (US1-AS3)",
			`{"recipient":"12345","channel":"sms","content":"` + strings.Repeat("a", 161) + `"}`, []string{"recipient", "content"}},
		{"email bad address (US1-AS4)",
			`{"recipient":"not-an-email","channel":"email","content":"hi"}`, []string{"recipient"}},
		{"email content over 100,000 chars (US1-AS4)",
			`{"recipient":"ada@example.com","channel":"email","content":"` + strings.Repeat("a", 100001) + `"}`, []string{"content"}},
		{"push empty token and 4,001 bytes (US1-AS5)",
			`{"recipient":"  ","channel":"push","content":"` + strings.Repeat("a", 4001) + `"}`, []string{"recipient", "content"}},
		{"no content and no template (US1-AS6)",
			`{"recipient":"+905551234567","channel":"sms"}`, []string{"content"}},
		{"unknown channel and priority",
			`{"recipient":"x","channel":"fax","content":"hi","priority":"urgent"}`, []string{"channel", "priority"}},
		{"idempotency key too long",
			`{"recipient":"+905551234567","channel":"sms","content":"hi","idempotency_key":"` + strings.Repeat("k", 256) + `"}`, []string{"idempotency_key"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := postJSON(t, url, tc.body, nil)
			if r.code != http.StatusBadRequest || errorCode(r) != "validation_failed" {
				t.Fatalf("got %d %v", r.code, r.body)
			}
			got := strings.Join(errorFields(r), ",")
			for _, f := range tc.wantFields {
				if !strings.Contains(","+got+",", ","+f+",") {
					t.Errorf("missing field %s in %s", f, got)
				}
			}
		})
	}
	if rowCount(t) != 0 {
		t.Fatal("invalid requests must store nothing")
	}
}

func TestAPICreateIdempotency(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	url := api.URL + "/api/v1/notifications"
	body := `{"recipient":"+905551234567","channel":"sms","content":"Your code is 1234"}`

	first := postJSON(t, url, body, map[string]string{"Idempotency-Key": "k1"})
	again := postJSON(t, url, body, map[string]string{"Idempotency-Key": "k1"})
	if first.code != 202 || again.code != 202 || again.body["id"] != first.body["id"] {
		t.Fatalf("replay: %d/%d ids %v/%v", first.code, again.code, first.body["id"], again.body["id"])
	}
	if again.header.Get("Idempotent-Replayed") != "true" || first.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("Idempotent-Replayed header: first=%q again=%q", first.header.Get("Idempotent-Replayed"), again.header.Get("Idempotent-Replayed"))
	}

	// The body field is equivalent to the header.
	viaBody := postJSON(t, url, `{"recipient":"+905551234567","channel":"sms","content":"Your code is 1234","idempotency_key":"k1"}`, nil)
	if viaBody.code != 202 || viaBody.body["id"] != first.body["id"] {
		t.Fatalf("body key replay: %d %v", viaBody.code, viaBody.body)
	}

	conflict := postJSON(t, url, `{"recipient":"+905551234567","channel":"sms","content":"Your code is 9999"}`,
		map[string]string{"Idempotency-Key": "k1"})
	if conflict.code != http.StatusConflict || errorCode(conflict) != "idempotency_conflict" {
		t.Fatalf("conflict: %d %v", conflict.code, conflict.body)
	}

	mismatch := postJSON(t, url, `{"recipient":"+905551234567","channel":"sms","content":"x","idempotency_key":"a"}`,
		map[string]string{"Idempotency-Key": "b"})
	if mismatch.code != http.StatusBadRequest || !strings.Contains(strings.Join(errorFields(mismatch), ","), "idempotency_key") {
		t.Fatalf("header/body mismatch: %d %v", mismatch.code, mismatch.body)
	}
	if rowCount(t) != 1 {
		t.Fatalf("rows = %d, want 1", rowCount(t))
	}
}

func TestAPICreateMalformed(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	resp, err := http.Post(api.URL+"/api/v1/notifications", "application/json", bytes.NewBufferString(`{"recipient":`)) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed JSON: %d", resp.StatusCode)
	}
}
