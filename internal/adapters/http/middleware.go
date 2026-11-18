package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
	"unicode"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
)

// HeaderCorrelationID is accepted on requests and always echoed on responses (FR-032).
const HeaderCorrelationID = "X-Correlation-ID"

// maxCorrelationIDLen matches the X-Correlation-ID parameter in api/openapi.yaml.
const maxCorrelationIDLen = 128

// RequestTimeout bounds handler work; it's below WriteTimeout so the error can still be written.
const RequestTimeout = 25 * time.Second

// Recover turns a handler panic into a logged 500 with the standard error body. Internals never
// reach the client (Constitution VIII).
func Recover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if p := recover(); p != nil {
					if p == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
						panic(p)
					}
					log.ErrorContext(r.Context(), "panic serving request", "panic", p, "stack", string(debug.Stack()))
					writeJSONError(w, http.StatusInternalServerError, "internal", "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// CorrelationID reads X-Correlation-ID or generates one, stores it in the request context (so
// every log line carries it) and echoes it on the response.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderCorrelationID)
		if !validCorrelationID(id) {
			id = newCorrelationID()
		}
		w.Header().Set(HeaderCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(logger.WithCorrelationID(r.Context(), id)))
	})
}

func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLen {
		return false
	}
	for _, c := range id {
		if c > unicode.MaxASCII || !unicode.IsPrint(c) {
			return false
		}
	}
	return true
}

func newCorrelationID() string {
	if u, err := uuid.NewV7(); err == nil {
		return u.String()
	}
	return uuid.NewString()
}

// AccessLog logs one line per request with the chi route pattern, never the raw path, so ids
// and PII stay out of the route field.
func AccessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			level := slog.LevelInfo
			if rw.status >= 500 {
				level = slog.LevelError
			}
			log.Log(r.Context(), level, "http request",
				"method", r.Method,
				"route", RoutePattern(r),
				"status", rw.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"bytes", rw.bytes,
			)
		})
	}
}

// RoutePattern returns the matched chi pattern, or "unmatched".
func RoutePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}

// Timeout attaches a deadline to the request context; I/O below it honours ctx.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// statusWriter records the status code and body size.
type statusWriter struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err //nolint:wrapcheck // transparent writer
}

// Unwrap lets http.ResponseController reach the underlying writer (needed for the /ws upgrade).
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
