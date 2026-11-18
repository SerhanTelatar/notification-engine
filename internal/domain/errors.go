package domain

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors. Adapters map them to the API error codes in api/openapi.yaml and always check
// them with errors.Is / errors.As.
var (
	ErrValidation          = errors.New("validation failed")
	ErrNotFound            = errors.New("not found")
	ErrInvalidTransition   = errors.New("invalid status transition")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	ErrNotCancellable      = errors.New("notification is not cancellable")
	ErrTemplateNameTaken   = errors.New("template name already taken")
	ErrTemplateNotFound    = errors.New("template not found")
)

// FieldError is one invalid field. Field uses JSON paths, e.g. "recipient" or "items[3].content".
type FieldError struct {
	Field   string
	Message string
}

// ValidationError collects every invalid field, not just the first (FR-005).
type ValidationError struct {
	Fields []FieldError
}

// Add records a field error.
func (v *ValidationError) Add(field, message string) {
	v.Fields = append(v.Fields, FieldError{Field: field, Message: message})
}

// Merge appends other's fields, prefixing each with prefix (e.g. "items[2].").
func (v *ValidationError) Merge(prefix string, other *ValidationError) {
	if other == nil {
		return
	}
	for _, f := range other.Fields {
		v.Add(prefix+f.Field, f.Message)
	}
}

// OrNil returns v as an error, or nil when no field failed.
func (v *ValidationError) OrNil() error {
	if v == nil || len(v.Fields) == 0 {
		return nil
	}
	return v
}

func (v *ValidationError) Error() string {
	parts := make([]string, len(v.Fields))
	for i, f := range v.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Is makes errors.Is(err, ErrValidation) true for any *ValidationError.
func (v *ValidationError) Is(target error) bool { return target == ErrValidation }

// NewFieldError is shorthand for a single-field validation error.
func NewFieldError(field, message string) *ValidationError {
	return &ValidationError{Fields: []FieldError{{Field: field, Message: message}}}
}

// NotCancellableError carries the status that blocked a cancel (409 with current_status).
type NotCancellableError struct {
	Current Status
}

func (e *NotCancellableError) Error() string {
	return fmt.Sprintf("%s: current status is %s", ErrNotCancellable, e.Current)
}

// Unwrap lets errors.Is(err, ErrNotCancellable) match.
func (e *NotCancellableError) Unwrap() error { return ErrNotCancellable }
