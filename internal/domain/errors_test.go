package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestValidationErrorCollectsAndMatches(t *testing.T) {
	var v ValidationError
	if v.OrNil() != nil {
		t.Fatal("empty ValidationError must be nil")
	}
	v.Add("recipient", "must be E.164")
	v.Add("content", "too long")

	item := NewFieldError("channel", "unknown")
	v.Merge("items[2].", item)

	err := fmt.Errorf("create: %w", v.OrNil())
	if !errors.Is(err, ErrValidation) {
		t.Fatal("errors.Is(ErrValidation) must match through wrapping")
	}
	var got *ValidationError
	if !errors.As(err, &got) || len(got.Fields) != 3 {
		t.Fatalf("errors.As failed or lost fields: %+v", got)
	}
	if got.Fields[2].Field != "items[2].channel" {
		t.Fatalf("merge prefix not applied: %+v", got.Fields[2])
	}
	if got.Error() != "validation failed: recipient: must be E.164; content: too long; items[2].channel: unknown" {
		t.Fatalf("message = %q", got.Error())
	}
}

func TestNotCancellableCarriesStatus(t *testing.T) {
	err := fmt.Errorf("cancel: %w", &NotCancellableError{Current: StatusSent})
	if !errors.Is(err, ErrNotCancellable) {
		t.Fatal("must match ErrNotCancellable")
	}
	var nc *NotCancellableError
	if !errors.As(err, &nc) || nc.Current != StatusSent {
		t.Fatalf("current status lost: %+v", nc)
	}
}

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{ErrValidation, ErrNotFound, ErrInvalidTransition, ErrIdempotencyConflict,
		ErrNotCancellable, ErrTemplateNameTaken, ErrTemplateNotFound,
		ErrInvalidChannel, ErrInvalidPriority, ErrInvalidStatus}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Fatalf("%v matches %v", a, b)
			}
		}
	}
}
