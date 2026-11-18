package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
)

func TestMapError(t *testing.T) {
	verr := &domain.ValidationError{}
	verr.Add("recipient", "must be E.164")
	verr.Add("content", "too long")

	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    string
		wantDetails int
		wantCurrent string
	}{
		{"validation keeps every field", fmt.Errorf("create: %w", verr), 400, CodeValidationFailed, 2, ""},
		{"not found", fmt.Errorf("get: %w", domain.ErrNotFound), 404, CodeNotFound, 0, ""},
		{"template not found", domain.ErrTemplateNotFound, 404, CodeNotFound, 0, ""},
		{"idempotency conflict", domain.ErrIdempotencyConflict, 409, CodeIdempotencyConflict, 0, ""},
		{"not cancellable carries status", fmt.Errorf("cancel: %w", &domain.NotCancellableError{Current: domain.StatusSent}),
			409, CodeNotCancellable, 0, "sent"},
		{"template name taken", domain.ErrTemplateNameTaken, 409, CodeTemplateNameTaken, 0, ""},
		{"not implemented", errNotImplemented, 501, CodeInternal, 0, ""},
		{"timeout", fmt.Errorf("query: %w", context.DeadlineExceeded), 503, CodeInternal, 0, ""},
		{"anything else is internal", errors.New("pq: relation does not exist"), 500, CodeInternal, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, p := mapError(tc.err)
			if status != tc.wantStatus || p.Code != tc.wantCode || len(p.Details) != tc.wantDetails || p.CurrentStatus != tc.wantCurrent {
				t.Fatalf("got %d %+v", status, p)
			}
		})
	}
}

func TestInternalErrorsAreLoggedNotLeaked(t *testing.T) {
	var logs bytes.Buffer
	h := responseErrorHandler(logger.NewWithWriter(&logs, "api", "info"))
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil), errors.New(`pq: password authentication failed for user "notify"`))

	if rec.Code != 500 || strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("leaked internals: %d %s", rec.Code, rec.Body)
	}
	if !strings.Contains(logs.String(), "password authentication failed") {
		t.Fatal("internal error must be logged")
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Message != "internal server error" {
		t.Fatalf("body = %s", rec.Body)
	}
}

func TestRequestErrorsThroughRouter(t *testing.T) {
	h := NewRouter(quiet, NewServer(quiet, health.New(0)))
	tests := []struct {
		name, method, path, body string
		wantField                string
		wantMsg                  string
	}{
		{"truncated JSON", http.MethodPost, "/api/v1/notifications", `{"recipient":`, "", "request body is not valid JSON"},
		{"invalid JSON", http.MethodPost, "/api/v1/notifications", `{"recipient" "x"}`, "", "request body is not valid JSON"},
		{"empty body", http.MethodPost, "/api/v1/notifications", ``, "", "request body is required"},
		{"wrong field type", http.MethodPost, "/api/v1/notifications", `{"recipient":42,"channel":"sms"}`, "recipient", ""},
		{"bad uuid path param", http.MethodGet, "/api/v1/notifications/not-a-uuid", "", "id", ""},
		{"bad query param", http.MethodGet, "/api/v1/notifications?limit=many", "", "limit", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			var body errorBody
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			if rec.Code != 400 || body.Error.Code != CodeValidationFailed {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
			if tc.wantField != "" && (len(body.Error.Details) != 1 || body.Error.Details[0].Field != tc.wantField) {
				t.Fatalf("details = %+v, want field %s", body.Error.Details, tc.wantField)
			}
			if tc.wantMsg != "" && body.Error.Message != tc.wantMsg {
				t.Fatalf("message = %q", body.Error.Message)
			}
			if strings.Contains(rec.Body.String(), "uuid.UUID") || strings.Contains(rec.Body.String(), "unmarshal") {
				t.Fatalf("Go internals leaked: %s", rec.Body)
			}
		})
	}
}
