package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

var req = ports.ProviderRequest{NotificationID: "n-1", CorrelationID: "c-1", To: "+905551234567",
	Channel: domain.ChannelSMS, Content: "Your code is 1234"}

func TestSendClassifiesResponses(t *testing.T) {
	tests := []struct {
		name        string
		handler     http.HandlerFunc
		wantOutcome domain.Outcome
		wantStatus  int
		wantMsgID   string
		wantRetry   time.Duration
		wantErrPart string
	}{
		{"202 accepted with message id", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(202)
			_, _ = w.Write([]byte(`{"messageId":"m-9","status":"accepted","timestamp":"2026-11-18T10:00:00Z"}`))
		}, domain.OutcomeSuccess, 202, "m-9", 0, ""},
		{"200 (webhook.site) is success", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) },
			domain.OutcomeSuccess, 200, "", 0, ""},
		{"2xx with unparsable body has no message id", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(202)
			_, _ = w.Write([]byte("<html>"))
		}, domain.OutcomeSuccess, 202, "", 0, ""},
		{"503 transient", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
			_, _ = w.Write([]byte("  upstream\n   overloaded "))
		}, domain.OutcomeTransient, 503, "", 0, "HTTP 503: upstream overloaded"},
		{"429 with Retry-After", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(429)
		}, domain.OutcomeTransient, 429, "", 2 * time.Second, "HTTP 429"},
		{"400 permanent", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400) },
			domain.OutcomePermanent, 400, "", 0, "HTTP 400"},
		{"redirect not followed is permanent", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		}, domain.OutcomePermanent, 302, "", 0, "HTTP 302"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			var observed domain.Outcome
			c := NewClient(srv.URL+"/send", time.Second, func(_ domain.Channel, o domain.Outcome, _ time.Duration) { observed = o })
			res := c.Send(context.Background(), req)
			if res.Outcome != tc.wantOutcome || res.HTTPStatus == nil || *res.HTTPStatus != tc.wantStatus {
				t.Fatalf("got %+v", res)
			}
			if got := deref(res.MessageID); got != tc.wantMsgID {
				t.Errorf("message id %q, want %q", got, tc.wantMsgID)
			}
			if res.RetryAfter != tc.wantRetry || !strings.Contains(res.Error, tc.wantErrPart) {
				t.Errorf("retry=%v error=%q", res.RetryAfter, res.Error)
			}
			if observed != tc.wantOutcome || res.Latency <= 0 {
				t.Errorf("latency not observed: %v %v", observed, res.Latency)
			}
		})
	}
}

func TestSendRequestShape(t *testing.T) {
	var got map[string]string
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(202)
	}))
	defer srv.Close()
	NewClient(srv.URL, time.Second, nil).Send(context.Background(), req)
	if got["to"] != req.To || got["channel"] != "sms" || got["content"] != req.Content || len(got) != 3 {
		t.Fatalf("body %v, want exactly {to, channel, content}", got)
	}
	if hdr.Get("Idempotency-Key") != "n-1" || hdr.Get("X-Correlation-ID") != "c-1" || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", hdr)
	}
}

func TestSendTransportFailuresAreTransientAndRedacted(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	res := NewClient(slow.URL+"/send?token=s3cret", 100*time.Millisecond, nil).Send(context.Background(), req)
	if res.Outcome != domain.OutcomeTransient || res.HTTPStatus != nil || !strings.Contains(res.Error, "timed out after 100ms") {
		t.Fatalf("timeout: %+v", res)
	}

	closed := httptest.NewServer(http.NotFoundHandler())
	addr := closed.URL
	closed.Close()
	res = NewClient(addr+"/send?token=s3cret", time.Second, nil).Send(context.Background(), req)
	if res.Outcome != domain.OutcomeTransient || res.HTTPStatus != nil {
		t.Fatalf("connection refused: %+v", res)
	}
	if strings.Contains(res.Error, "s3cret") || strings.Contains(res.Error, addr) {
		t.Fatalf("provider URL leaked into the error: %q", res.Error)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Providers echo requests in error bodies; the recipient and content must not reach last_error
// or the logs, raw or JSON-escaped.
func TestSnippetMasksRecipientAndContent(t *testing.T) {
	content := "line one\n\"quoted\" code 918273"
	esc, _ := json.Marshal(content)
	body := []byte(`{"error":"bad","to":"+905559871234","content":` + string(esc) + `,"raw":"+905559871234"}`)
	got := snippet(body, "+905559871234", content)
	if strings.Contains(got, "+905559871234") || strings.Contains(got, "918273") {
		t.Fatalf("snippet leaks: %s", got)
	}
	if !strings.Contains(got, `"error":"bad"`) {
		t.Fatalf("snippet lost the error text: %s", got)
	}
	if got := snippet([]byte("this is fine"), "hi"); got != ": this is fine" {
		t.Fatalf("short values must not be masked: %q", got)
	}
}
