//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// seedRows inserts n rows of one channel and status, created 1 ms apart going back from base.
func seedRows(t testing.TB, n int, channel, status string, base time.Time) {
	t.Helper()
	recipient := map[string]string{"sms": "+905551234567", "email": "ada@example.com", "push": "token"}[channel]
	_, err := env.Pool.Exec(context.Background(), `
		INSERT INTO notifications (id, payload_hash, channel, recipient, content, priority, status,
		                           max_attempts, next_attempt_at, correlation_id, created_at, updated_at)
		SELECT gen_random_uuid(), sha256(($1 || $2 || i)::bytea), $1, $3, 'seed', 2, $2, 5, now(), 'seed',
		       $4::timestamptz - i * interval '1 millisecond', now()
		FROM generate_series(1, $5) AS i`, channel, status, recipient, base, n)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

type page struct {
	ids     []string
	created []time.Time
	next    string
}

func listPage(t testing.TB, base string, q url.Values) (int, page, map[string]any) {
	t.Helper()
	code, body := getJSON(t, base+"/api/v1/notifications?"+q.Encode())
	var p page
	if items, ok := body["items"].([]any); ok {
		for _, raw := range items {
			it := raw.(map[string]any)
			p.ids = append(p.ids, it["id"].(string))
			ts, _ := time.Parse(time.RFC3339Nano, it["created_at"].(string))
			p.created = append(p.created, ts)
		}
	}
	if c, ok := body["next_cursor"].(string); ok {
		p.next = c
	}
	return code, p, body
}

func TestAPIListPagesWithoutGapsOrDuplicates(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	base := time.Now().Add(-time.Hour).UTC()
	seedRows(t, 250, "sms", "pending", base)

	seen := map[string]bool{}
	var sizes []int
	var last time.Time
	q := url.Values{"limit": {"100"}}
	for pageNo := 0; ; pageNo++ {
		code, p, body := listPage(t, api.URL, q)
		if code != http.StatusOK {
			t.Fatalf("page %d: %d %v", pageNo, code, body)
		}
		sizes = append(sizes, len(p.ids))
		for i, id := range p.ids {
			if seen[id] {
				t.Fatalf("duplicate %s on page %d", id, pageNo)
			}
			seen[id] = true
			if !last.IsZero() && p.created[i].After(last) {
				t.Fatalf("not newest first at page %d item %d", pageNo, i)
			}
			last = p.created[i]
		}
		if pageNo == 0 {
			// New notifications arriving while paging must not shift later pages (keyset, R13).
			seedRows(t, 30, "sms", "pending", time.Now().UTC())
		}
		if p.next == "" {
			break
		}
		q.Set("cursor", p.next)
		if pageNo > 5 {
			t.Fatal("paging never ended")
		}
	}
	if len(sizes) != 3 || sizes[0] != 100 || sizes[1] != 100 || sizes[2] != 50 || len(seen) != 250 {
		t.Fatalf("pages %v, distinct %d: want 100/100/50 covering all 250 originals (US6-AS1)", sizes, len(seen))
	}
}

func TestAPIListFilters(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	base := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	seedRows(t, 5, "email", "failed", base)
	seedRows(t, 4, "sms", "failed", base)
	seedRows(t, 3, "email", "pending", base)

	_, p, _ := listPage(t, api.URL, url.Values{"status": {"failed"}, "channel": {"email"}})
	if len(p.ids) != 5 || p.next != "" {
		t.Fatalf("status=failed&channel=email: %d items next=%q, want 5 and no cursor (US6-AS2)", len(p.ids), p.next)
	}

	// Rows are at base-1ms ... base-5ms. from is inclusive, to exclusive (US6-AS3).
	from := base.Add(-3 * time.Millisecond).Format(time.RFC3339Nano)
	to := base.Add(-1 * time.Millisecond).Format(time.RFC3339Nano)
	_, p, _ = listPage(t, api.URL, url.Values{"channel": {"email"}, "status": {"failed"}, "from": {from}, "to": {to}})
	if len(p.ids) != 2 {
		t.Fatalf("[base-3ms, base-1ms): %d items, want 2 (base-3ms and base-2ms)", len(p.ids))
	}

	_, p, _ = listPage(t, api.URL, url.Values{"status": {"sent"}})
	if len(p.ids) != 0 || p.next != "" {
		t.Fatalf("empty result: %d items next=%q", len(p.ids), p.next)
	}

	_, p, _ = listPage(t, api.URL, url.Values{})
	if len(p.ids) != 12 {
		t.Fatalf("default page: %d items, want all 12 (default limit 20)", len(p.ids))
	}
}

func TestAPIListRejectsBadQueries(t *testing.T) {
	env.Truncate(t)
	api := newAPI(t)
	for name, q := range map[string]url.Values{
		"limit above 100": {"limit": {"101"}},
		"limit zero":      {"limit": {"0"}},
		"bad cursor":      {"cursor": {"garbage"}},
		"unknown status":  {"status": {"delivered"}},
		"unknown channel": {"channel": {"fax"}},
		"from after to":   {"from": {"2026-11-18T10:00:00Z"}, "to": {"2026-11-18T09:00:00Z"}},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, body := listPage(t, api.URL, q)
			if code != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "validation_failed" {
				t.Fatalf("%v: %d %v", q, code, body)
			}
		})
	}
}
