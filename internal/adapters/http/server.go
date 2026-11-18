// Package httpapi is the HTTP adapter. The routing, request decoding and response types are
// generated from api/openapi.yaml (package gen); this package implements the strict server
// interface, middleware and error mapping (Constitution I).
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
)

// Server timeouts and limits (research R15).
const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	IdleTimeout       = 60 * time.Second
	// MaxBodyBytes fits a 1,000-item batch of 100 KB emails with JSON overhead.
	MaxBodyBytes = 10 << 20
)

// errNotImplemented marks operations whose user story hasn't landed. The response error handler
// maps it to 501, which isn't a declared response in api/openapi.yaml.
var errNotImplemented = errors.New("operation not implemented yet")

// Server implements gen.StrictServerInterface. Operations not built yet return errNotImplemented.
type Server struct {
	log *slog.Logger
}

var _ gen.StrictServerInterface = (*Server)(nil)

// NewServer returns the strict-server implementation.
func NewServer(log *slog.Logger) *Server { return &Server{log: log} }

// NewRouter mounts the generated routes on chi.
func NewRouter(log *slog.Logger, s gen.StrictServerInterface) chi.Router {
	r := chi.NewRouter()
	r.Use(Recover(log), CorrelationID, AccessLog(log), Timeout(RequestTimeout))
	strict := gen.NewStrictHandlerWithOptions(s, nil, gen.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeJSONError(w, http.StatusBadRequest, "validation_failed", err.Error())
		},
		ResponseErrorHandlerFunc: responseErrorHandler(log),
	})
	gen.HandlerWithOptions(strict, gen.ChiServerOptions{
		BaseRouter: r,
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeJSONError(w, http.StatusBadRequest, "validation_failed", err.Error())
		},
	})
	return r
}

// NewHTTPServer wraps a handler with the server timeouts and body limit.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           http.MaxBytesHandler(h, MaxBodyBytes),
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
	}
}

func responseErrorHandler(log *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, errNotImplemented) {
			writeJSONError(w, http.StatusNotImplemented, "internal", "this operation is not implemented yet")
			return
		}
		log.ErrorContext(r.Context(), "request failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

// --- Operations: each returns errNotImplemented until its user story lands. ---

func (s *Server) CreateNotification(context.Context, gen.CreateNotificationRequestObject) (gen.CreateNotificationResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) ListNotifications(context.Context, gen.ListNotificationsRequestObject) (gen.ListNotificationsResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetNotification(context.Context, gen.GetNotificationRequestObject) (gen.GetNotificationResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) CancelNotification(context.Context, gen.CancelNotificationRequestObject) (gen.CancelNotificationResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) CreateBatch(context.Context, gen.CreateBatchRequestObject) (gen.CreateBatchResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetBatch(context.Context, gen.GetBatchRequestObject) (gen.GetBatchResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) CancelBatch(context.Context, gen.CancelBatchRequestObject) (gen.CancelBatchResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) CreateTemplate(context.Context, gen.CreateTemplateRequestObject) (gen.CreateTemplateResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) ListTemplates(context.Context, gen.ListTemplatesRequestObject) (gen.ListTemplatesResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetTemplate(context.Context, gen.GetTemplateRequestObject) (gen.GetTemplateResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetMetricsSummary(context.Context, gen.GetMetricsSummaryRequestObject) (gen.GetMetricsSummaryResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetLiveness(context.Context, gen.GetLivenessRequestObject) (gen.GetLivenessResponseObject, error) {
	return nil, errNotImplemented
}

func (s *Server) GetReadiness(context.Context, gen.GetReadinessRequestObject) (gen.GetReadinessResponseObject, error) {
	return nil, errNotImplemented
}
