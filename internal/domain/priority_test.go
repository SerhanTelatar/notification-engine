package domain

import (
	"errors"
	"testing"
)

func TestParsePriority(t *testing.T) {
	tests := []struct {
		in      string
		want    Priority
		wantStr string
		wantErr bool
	}{
		{"high", PriorityHigh, "high", false},
		{"normal", PriorityNormal, "normal", false},
		{"low", PriorityLow, "low", false},
		{"", PriorityNormal, "normal", false}, // default (FR-001)
		{"urgent", 0, "", true},
		{"HIGH", 0, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParsePriority(tc.in)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidPriority) {
					t.Fatalf("err = %v, want ErrInvalidPriority", err)
				}
				return
			}
			if err != nil || got != tc.want || got.String() != tc.wantStr {
				t.Fatalf("ParsePriority(%q) = %v (%s), %v", tc.in, got, got, err)
			}
		})
	}
}

func TestPriorityOrderingAndStorage(t *testing.T) {
	if PriorityHigh <= PriorityNormal || PriorityNormal <= PriorityLow {
		t.Fatal("higher priority must sort higher (relay ORDER BY priority DESC)")
	}
	for _, n := range []int16{1, 2, 3} {
		p, err := PriorityFromInt(n)
		if err != nil || int16(p) != n {
			t.Fatalf("round trip %d failed: %v", n, err)
		}
	}
	for _, n := range []int16{0, 4, -1} {
		if _, err := PriorityFromInt(n); !errors.Is(err, ErrInvalidPriority) {
			t.Fatalf("PriorityFromInt(%d) must fail", n)
		}
	}
}
