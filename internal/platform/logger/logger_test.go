package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(b), &m); err != nil {
		t.Fatalf("not one JSON object: %q: %v", b, err)
	}
	return m
}

func TestContextIDsAreStamped(t *testing.T) {
	tests := []struct {
		name   string
		ctx    func() context.Context
		want   map[string]string
		absent []string
	}{
		{
			name:   "no ids",
			ctx:    context.Background,
			want:   map[string]string{"service": "api", "msg": "hello"},
			absent: []string{FieldCorrelationID, FieldNotificationID, FieldBatchID},
		},
		{
			name:   "correlation only",
			ctx:    func() context.Context { return WithCorrelationID(context.Background(), "c-1") },
			want:   map[string]string{FieldCorrelationID: "c-1"},
			absent: []string{FieldNotificationID},
		},
		{
			name: "all ids",
			ctx: func() context.Context {
				ctx := WithCorrelationID(context.Background(), "c-2")
				ctx = WithNotificationID(ctx, "n-2")
				return WithBatchID(ctx, "b-2")
			},
			want: map[string]string{FieldCorrelationID: "c-2", FieldNotificationID: "n-2", FieldBatchID: "b-2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			NewWithWriter(&buf, "api", "info").InfoContext(tc.ctx(), "hello")
			got := decode(t, buf.Bytes())
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s = %v, want %q", k, got[k], v)
				}
			}
			for _, k := range tc.absent {
				if _, ok := got[k]; ok {
					t.Errorf("%s should be absent", k)
				}
			}
		})
	}
}

func TestIDsSurviveWithAndGroups(t *testing.T) {
	var buf bytes.Buffer
	l := NewWithWriter(&buf, "worker", "info").With("component", "relay")
	l.InfoContext(WithCorrelationID(context.Background(), "c-9"), "tick", Err(errors.New("boom")))
	got := decode(t, buf.Bytes())
	if got[FieldCorrelationID] != "c-9" || got["component"] != "relay" || got[FieldError] != "boom" {
		t.Fatalf("unexpected record: %v", got)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := NewWithWriter(&buf, "api", "warn")
	l.Info("dropped")
	if buf.Len() != 0 {
		t.Fatalf("info logged at warn level: %s", buf.String())
	}
	if ParseLevel("nonsense") != slog.LevelInfo {
		t.Fatal("unknown level must default to info")
	}
}

func TestFromReturnsAttachedLogger(t *testing.T) {
	var buf bytes.Buffer
	l := NewWithWriter(&buf, "api", "info")
	From(WithLogger(context.Background(), l)).Info("via ctx")
	if decode(t, buf.Bytes())["msg"] != "via ctx" {
		t.Fatal("From did not return the attached logger")
	}
	if From(context.Background()) == nil {
		t.Fatal("From must never return nil")
	}
	if CorrelationID(context.Background()) != "" {
		t.Fatal("empty ctx has no correlation id")
	}
}

// An id already on the logger (With) or the record isn't added again from ctx.
func TestContextIDsAreNotDuplicated(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithWriter(&buf, "worker", "info")
	ctx := WithNotificationID(WithCorrelationID(context.Background(), "c-1"), "n-1")

	log.With(FieldCorrelationID, "c-1").InfoContext(ctx, "with")
	log.InfoContext(ctx, "record", FieldNotificationID, "n-1")
	log.InfoContext(ctx, "plain")
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		for _, key := range []string{FieldCorrelationID, FieldNotificationID} {
			if n := strings.Count(line, `"`+key+`":`); n != 1 {
				t.Fatalf("%s appears %d times: %s", key, n, line)
			}
		}
	}
}
