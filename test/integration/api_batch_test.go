//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func batchBody(n int, invalid map[int]bool) string {
	items := make([]string, n)
	for i := range n {
		recipient := "+905551234567"
		if invalid[i] {
			recipient = "not-e164"
		}
		items[i] = fmt.Sprintf(`{"recipient":%q,"channel":"sms","content":"item %d"}`, recipient, i)
	}
	return `{"items":[` + strings.Join(items, ",") + `]}`
}

func batchRowCount(t testing.TB) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM batches`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAPIBatchPartialSuccess(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	r := postJSON(t, api.URL+"/api/v1/notifications/batch", batchBody(10, map[int]bool{1: true, 4: true, 8: true}),
		map[string]string{"X-Correlation-ID": "it-batch"})
	if r.code != http.StatusAccepted || r.body["accepted"] != float64(7) || r.body["rejected"] != float64(3) {
		t.Fatalf("got %d %v", r.code, r.body)
	}
	items := r.body["items"].([]any)
	if len(items) != 10 {
		t.Fatalf("items %d", len(items))
	}
	for i, raw := range items {
		it := raw.(map[string]any)
		rejected := i == 1 || i == 4 || i == 8
		if it["index"] != float64(i) || it["accepted"] == rejected {
			t.Fatalf("item %d: %v", i, it)
		}
		if rejected {
			errs := it["errors"].([]any)
			if errs[0].(map[string]any)["field"] != "recipient" {
				t.Fatalf("item %d errors %v", i, errs)
			}
		} else if it["notification_id"] == nil || it["status"] != "pending" {
			t.Fatalf("accepted item %d: %v", i, it)
		}
	}
	var stored, withCorrelation int
	_ = env.Pool.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE correlation_id = 'it-batch')
		FROM notifications WHERE batch_id = $1`, r.body["batch_id"]).Scan(&stored, &withCorrelation)
	if stored != 7 || withCorrelation != 7 {
		t.Fatalf("stored %d (correlated %d), want 7", stored, withCorrelation)
	}
}

func TestAPIBatchOfAThousandIsFast(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	body := batchBody(1000, nil)
	start := time.Now()
	r := postJSON(t, api.URL+"/api/v1/notifications/batch", body, nil)
	elapsed := time.Since(start)
	t.Logf("1,000-item batch acknowledged in %v", elapsed.Round(time.Millisecond))
	if r.code != http.StatusAccepted || r.body["accepted"] != float64(1000) {
		t.Fatalf("got %d accepted=%v", r.code, r.body["accepted"])
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took %v, want < 2 s (SC-002)", elapsed)
	}
	if rowCount(t) != 1000 {
		t.Fatalf("rows %d", rowCount(t))
	}
}

func TestAPIBatchRejections(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	url := api.URL + "/api/v1/notifications/batch"

	tooMany := postJSON(t, url, batchBody(1001, nil), nil)
	if tooMany.code != http.StatusBadRequest || errorCode(tooMany) != "validation_failed" {
		t.Fatalf("1,001 items: %d %v", tooMany.code, tooMany.body)
	}
	empty := postJSON(t, url, `{"items":[]}`, nil)
	if empty.code != http.StatusBadRequest {
		t.Fatalf("empty batch: %d", empty.code)
	}
	allBad := postJSON(t, url, batchBody(3, map[int]bool{0: true, 1: true, 2: true}), nil)
	if allBad.code != http.StatusBadRequest || !strings.Contains(strings.Join(errorFields(allBad), ","), "items[2].recipient") {
		t.Fatalf("all invalid: %d %v", allBad.code, allBad.body)
	}
	if batchRowCount(t) != 0 || rowCount(t) != 0 {
		t.Fatalf("batches %d notifications %d: rejected batches must store nothing", batchRowCount(t), rowCount(t))
	}
}

func TestAPIGetBatch(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	created := postJSON(t, api.URL+"/api/v1/notifications/batch", batchBody(5, map[int]bool{3: true}), nil)
	id := created.body["batch_id"].(string)
	// One item has been sent in the meantime.
	first := created.body["items"].([]any)[0].(map[string]any)["notification_id"].(string)
	if _, err := env.Pool.Exec(context.Background(),
		`UPDATE notifications SET status = 'sent', sent_at = now(), attempts = 1 WHERE id = $1`, first); err != nil {
		t.Fatal(err)
	}

	code, body := getJSON(t, api.URL+"/api/v1/batches/"+id)
	if code != http.StatusOK || body["batch_id"] != id || body["total"] != float64(4) {
		t.Fatalf("%d %v", code, body)
	}
	counts := body["counts"].(map[string]any)
	sum := 0.0
	for _, v := range counts {
		sum += v.(float64)
	}
	raw, _ := json.Marshal(counts)
	if sum != 4 || counts["sent"] != float64(1) || counts["pending"] != float64(3) || len(body["items"].([]any)) != 4 {
		t.Fatalf("counts %s, items %d: counts must sum to the accepted total", raw, len(body["items"].([]any)))
	}

	code, body = getJSON(t, api.URL+"/api/v1/batches/0193a3b0-0000-7000-8000-000000000001")
	if code != http.StatusNotFound || body["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("unknown batch: %d %v", code, body)
	}
}
