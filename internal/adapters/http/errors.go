package httpapi

import (
	"encoding/json"
	"net/http"
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

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	writeError(w, status, errorPayload{Code: code, Message: message})
}

func writeError(w http.ResponseWriter, status int, p errorPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: p})
}
