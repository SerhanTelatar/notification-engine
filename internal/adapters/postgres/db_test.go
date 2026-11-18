package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

func TestTranslate(t *testing.T) {
	other := errors.New("connection reset")
	tests := []struct {
		name       string
		in         error
		wantIs     error
		constraint string
	}{
		{"nil stays nil", nil, nil, ""},
		{"no rows is not found", pgx.ErrNoRows, domain.ErrNotFound, ""},
		{"wrapped no rows", fmt.Errorf("scan: %w", pgx.ErrNoRows), domain.ErrNotFound, ""},
		{"unique violation", &pgconn.PgError{Code: "23505", ConstraintName: "notifications_idempotency_key_key"},
			ErrUniqueViolation, "notifications_idempotency_key_key"},
		{"check violation passes through", &pgconn.PgError{Code: "23514"}, nil, ""},
		{"other errors keep their identity", other, other, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translate(tc.in, "op")
			if tc.in == nil {
				if got != nil {
					t.Fatalf("got %v", got)
				}
				return
			}
			if tc.wantIs != nil && !errors.Is(got, tc.wantIs) {
				t.Fatalf("errors.Is(%v, %v) = false", got, tc.wantIs)
			}
			if tc.constraint != "" {
				var u *UniqueViolationError
				if !errors.As(got, &u) || u.Constraint != tc.constraint {
					t.Fatalf("constraint lost: %v", got)
				}
			}
			if errors.Is(got, domain.ErrNotFound) && !errors.Is(tc.wantIs, domain.ErrNotFound) {
				t.Fatal("must not invent not-found")
			}
		})
	}
}
