package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// Error codes: the enum of components.schemas.Error.code in api/openapi.yaml.
const (
	CodeValidationFailed    = "validation_failed"
	CodeNotFound            = "not_found"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeNotCancellable      = "not_cancellable"
	CodeTemplateNameTaken   = "template_name_taken"
	CodeInternal            = "internal"
)

// errorBody is the single error shape of api/openapi.yaml (components.schemas.Error).
type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code          string       `json:"code"`
	Message       string       `json:"message"`
	Details       []fieldError `json:"details,omitempty"`
	CurrentStatus string       `json:"current_status,omitempty"`
}

type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// mapError turns any error into a status and a client-safe body. Only domain-shaped errors pass
// their message through; anything else becomes a generic 500 (Constitution VIII).
func mapError(err error) (int, errorPayload) {
	var (
		verr *domain.ValidationError
		nc   *domain.NotCancellableError
	)
	switch {
	case errors.As(err, &verr):
		p := errorPayload{Code: CodeValidationFailed, Message: "request validation failed"}
		for _, f := range verr.Fields {
			p.Details = append(p.Details, fieldError{Field: f.Field, Message: f.Message})
		}
		return http.StatusBadRequest, p
	case errors.As(err, &nc):
		return http.StatusConflict, errorPayload{Code: CodeNotCancellable,
			Message: "notification can no longer be cancelled", CurrentStatus: string(nc.Current)}
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrTemplateNotFound):
		return http.StatusNotFound, errorPayload{Code: CodeNotFound, Message: "resource not found"}
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return http.StatusConflict, errorPayload{Code: CodeIdempotencyConflict,
			Message: "idempotency key was already used with a different payload"}
	case errors.Is(err, domain.ErrTemplateNameTaken):
		return http.StatusConflict, errorPayload{Code: CodeTemplateNameTaken, Message: "a template with this name already exists"}
	case errors.Is(err, errNotImplemented):
		return http.StatusNotImplemented, errorPayload{Code: CodeInternal, Message: "this operation is not implemented yet"}
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, errorPayload{Code: CodeInternal, Message: "request timed out, retry later"}
	default:
		return http.StatusInternalServerError, errorPayload{Code: CodeInternal, Message: "internal server error"}
	}
}

// mapRequestError handles decoding and parameter-binding failures from the generated code. The
// raw error text can name Go types, so only the field name and a fixed message reach the client.
func mapRequestError(err error) errorPayload {
	p := errorPayload{Code: CodeValidationFailed, Message: "request is malformed"}

	var (
		maxBytes  *http.MaxBytesError
		syntax    *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		badFormat *gen.InvalidParamFormatError
		required  *gen.RequiredParamError
		reqHeader *gen.RequiredHeaderError
		tooMany   *gen.TooManyValuesForParamError
		unescaped *gen.UnescapedCookieParamError
		unmarshal *gen.UnmarshalingParamError
	)
	switch {
	case errors.As(err, &maxBytes):
		p.Message = "request body is too large"
	case errors.Is(err, io.EOF):
		p.Message = "request body is required"
	case errors.As(err, &syntax), errors.Is(err, io.ErrUnexpectedEOF):
		p.Message = "request body is not valid JSON"
	case errors.As(err, &typeErr):
		p.Message = "request body has a field of the wrong type"
		p.Details = []fieldError{{Field: typeErr.Field, Message: "must be of type " + typeErr.Type.String()}}
	case errors.As(err, &badFormat):
		p.Details = []fieldError{{Field: badFormat.ParamName, Message: "has an invalid format"}}
	case errors.As(err, &required):
		p.Details = []fieldError{{Field: required.ParamName, Message: "is required"}}
	case errors.As(err, &reqHeader):
		p.Details = []fieldError{{Field: reqHeader.ParamName, Message: "header is required"}}
	case errors.As(err, &tooMany):
		p.Details = []fieldError{{Field: tooMany.ParamName, Message: "must be given once"}}
	case errors.As(err, &unescaped):
		p.Details = []fieldError{{Field: unescaped.ParamName, Message: "is not properly escaped"}}
	case errors.As(err, &unmarshal):
		p.Details = []fieldError{{Field: unmarshal.ParamName, Message: "has an invalid value"}}
	default:
		p.Message = "request body could not be decoded"
	}
	return p
}

// responseErrorHandler is the strict server's ResponseErrorHandlerFunc: handlers return domain
// errors and this maps them. Server-side failures are logged with the wrapped chain.
func responseErrorHandler(log *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		status, p := mapError(err)
		if status >= 500 && status != http.StatusNotImplemented {
			log.ErrorContext(r.Context(), "request failed", "error", err)
		}
		writeError(w, status, p)
	}
}

// requestErrorHandler covers body decoding and parameter binding (strict and chi wrappers).
func requestErrorHandler(log *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		log.DebugContext(r.Context(), "bad request", "error", err)
		writeError(w, http.StatusBadRequest, mapRequestError(err))
	}
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	writeError(w, status, errorPayload{Code: code, Message: message})
}

func writeError(w http.ResponseWriter, status int, p errorPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: p})
}
