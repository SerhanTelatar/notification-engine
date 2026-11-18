package domain

import (
	"errors"
	"slices"
	"testing"
)

// TestCanTransitionAllPairs checks all 64 (from, to) pairs against FR-012.
func TestCanTransitionAllPairs(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusQueued}:      true,
		{StatusPending, StatusCancelled}:   true,
		{StatusScheduled, StatusQueued}:    true,
		{StatusScheduled, StatusCancelled}: true,
		{StatusQueued, StatusSending}:      true,
		{StatusQueued, StatusCancelled}:    true,
		{StatusSending, StatusSent}:        true,
		{StatusSending, StatusRetrying}:    true,
		{StatusSending, StatusFailed}:      true,
		{StatusRetrying, StatusQueued}:     true,
		{StatusRetrying, StatusCancelled}:  true,
	}
	pairs := 0
	for _, from := range Statuses {
		for _, to := range Statuses {
			pairs++
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	if pairs != 64 {
		t.Fatalf("checked %d pairs, want 64", pairs)
	}
}

// Rules the spec states explicitly, as readable named cases.
func TestSpecRules(t *testing.T) {
	tests := []struct {
		name     string
		from, to Status
		want     bool
	}{
		{"cancel never from sending (US5-AS2)", StatusSending, StatusCancelled, false},
		{"breaker deferral must pass through sending", StatusQueued, StatusRetrying, false},
		{"retrying goes back through the relay", StatusRetrying, StatusQueued, true},
		{"no resurrection of sent", StatusSent, StatusQueued, false},
		{"no resurrection of cancelled", StatusCancelled, StatusPending, false},
		{"no self transition", StatusQueued, StatusQueued, false},
		{"scheduled cannot jump to sending", StatusScheduled, StatusSending, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanTransition(tc.from, tc.to); got != tc.want {
				t.Fatalf("CanTransition(%s, %s) = %v", tc.from, tc.to, got)
			}
		})
	}
}

func TestTerminalAndSets(t *testing.T) {
	for _, s := range Statuses {
		want := s == StatusSent || s == StatusFailed || s == StatusCancelled
		if s.IsTerminal() != want {
			t.Errorf("%s.IsTerminal() = %v", s, s.IsTerminal())
		}
	}
	if !slices.Equal(AllowedFrom(StatusCancelled), CancellableFrom) {
		t.Errorf("CancellableFrom %v disagrees with transitions %v", CancellableFrom, AllowedFrom(StatusCancelled))
	}
	if !slices.Equal(AllowedFrom(StatusQueued), DueFrom) {
		t.Errorf("DueFrom %v disagrees with transitions %v", DueFrom, AllowedFrom(StatusQueued))
	}
	if !slices.Equal(AllowedFrom(StatusSending), SendableFrom) {
		t.Errorf("SendableFrom %v disagrees with transitions %v", SendableFrom, AllowedFrom(StatusSending))
	}
	if !StatusQueued.In(InFlight) || StatusRetrying.In(InFlight) {
		t.Error("InFlight must be exactly queued and sending")
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range Statuses {
		got, err := ParseStatus(string(s))
		if err != nil || got != s {
			t.Fatalf("ParseStatus(%q) = %q, %v", s, got, err)
		}
	}
	if _, err := ParseStatus("delivered"); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("unknown status must fail, got %v", err)
	}
}
