package app

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

func TestCursorRoundTrip(t *testing.T) {
	c := ports.Cursor{CreatedAt: time.Date(2026, 11, 18, 10, 0, 0, 123456000, time.UTC), ID: "0193a3b0-0000-7000-8000-000000000001"}
	s := EncodeCursor(c)
	if strings.ContainsAny(s, "+/=") {
		t.Fatalf("cursor %q must be URL-safe base64 without padding", s)
	}
	got, err := DecodeCursor(s)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("round trip %+v, want %+v (microsecond precision kept)", got, c)
	}
}

func TestCursorRejectsBadInput(t *testing.T) {
	valid := EncodeCursor(ports.Cursor{CreatedAt: time.Now(), ID: "0193a3b0-0000-7000-8000-000000000001"})
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	tests := map[string]string{
		"not base64":       "!!!",
		"truncated":        valid[:len(valid)/2],
		"not json":         enc("hello"),
		"missing id":       enc(`{"c":"2026-11-18T10:00:00Z"}`),
		"missing time":     enc(`{"i":"0193a3b0-0000-7000-8000-000000000001"}`),
		"bad id":           enc(`{"c":"2026-11-18T10:00:00Z","i":"not-a-uuid"}`),
		"bad time":         enc(`{"c":"yesterday","i":"0193a3b0-0000-7000-8000-000000000001"}`),
		"tampered one bit": valid[:len(valid)-2] + "zz",
		"empty":            "",
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeCursor(in)
			var v *domain.ValidationError
			if !errors.As(err, &v) || v.Fields[0].Field != "cursor" {
				t.Fatalf("DecodeCursor(%q) = %v, want ValidationError on cursor", in, err)
			}
		})
	}
}
