// Package logger provides structured JSON logging (log/slog) whose handler stamps every record
// with the correlation, notification and batch ids carried in the context (Constitution IV,
// contracts/observability.md § Logs). Callers use the *Context methods, e.g. log.InfoContext(ctx,
// ...), and the ids can't be forgotten.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type ctxKey int

const (
	keyCorrelationID ctxKey = iota
	keyNotificationID
	keyBatchID
	keyLogger
)

// Field names shared by every log line (contracts/observability.md).
const (
	FieldService        = "service"
	FieldCorrelationID  = "correlation_id"
	FieldNotificationID = "notification_id"
	FieldBatchID        = "batch_id"
	FieldError          = "error"
)

// New returns a JSON logger to stdout for the given service ("api", "worker", ...).
func New(service, level string) *slog.Logger {
	return NewWithWriter(os.Stdout, service, level)
}

// NewWithWriter is New with an explicit destination (tests capture output this way).
func NewWithWriter(w io.Writer, service, level string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: ParseLevel(level)})
	return slog.New(contextHandler{inner: h}).With(FieldService, service)
}

// ParseLevel maps debug|info|warn|error to a slog level; unknown values mean info.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithCorrelationID stores the correlation id; every log record made with ctx carries it.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyCorrelationID, id)
}

// CorrelationID returns the correlation id in ctx, or "".
func CorrelationID(ctx context.Context) string { return str(ctx, keyCorrelationID) }

// WithNotificationID stores the notification id being processed.
func WithNotificationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyNotificationID, id)
}

// WithBatchID stores the batch id being processed.
func WithBatchID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyBatchID, id)
}

// WithLogger attaches a logger to ctx for code that has no logger injected.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, keyLogger, l)
}

// From returns the logger attached to ctx. Without one it returns a stderr JSON logger, so a
// missing injection degrades to visible output instead of silence.
func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(keyLogger).(*slog.Logger); ok && l != nil {
		return l
	}
	return NewWithWriter(os.Stderr, "unknown", "info")
}

// Err formats an error attribute consistently.
func Err(err error) slog.Attr { return slog.Any(FieldError, err) }

func str(ctx context.Context, k ctxKey) string {
	if v, ok := ctx.Value(k).(string); ok {
		return v
	}
	return ""
}

// contextHandler copies ids from the record's context into the record, unless the logger
// (With) or the record already carries that key: JSON with a repeated key is ambiguous.
type contextHandler struct {
	inner  slog.Handler
	preset map[string]bool // keys added with WithAttrs at the top level
}

func (h contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		present := func(name string) bool {
			if h.preset[name] {
				return true
			}
			found := false
			r.Attrs(func(a slog.Attr) bool {
				found = a.Key == name
				return !found
			})
			return found
		}
		for _, f := range []struct {
			key  ctxKey
			name string
		}{
			{keyCorrelationID, FieldCorrelationID},
			{keyNotificationID, FieldNotificationID},
			{keyBatchID, FieldBatchID},
		} {
			if v := str(ctx, f.key); v != "" && !present(f.name) {
				r.AddAttrs(slog.String(f.name, v))
			}
		}
	}
	return h.inner.Handle(ctx, r) //nolint:wrapcheck // handler chain, nothing to add
}

func (h contextHandler) WithAttrs(as []slog.Attr) slog.Handler {
	preset := make(map[string]bool, len(h.preset)+len(as))
	for k := range h.preset {
		preset[k] = true
	}
	for _, a := range as {
		preset[a.Key] = true
	}
	return contextHandler{inner: h.inner.WithAttrs(as), preset: preset}
}

// WithGroup nests later attributes, so ids added from ctx (top level) can't collide with them.
func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{inner: h.inner.WithGroup(name), preset: h.preset}
}
