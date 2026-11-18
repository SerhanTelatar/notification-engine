package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidStatus is returned for an unknown status value.
var ErrInvalidStatus = errors.New("invalid status")

// Status is the notification lifecycle state (data-model.md notifications.status, FR-012).
type Status string

const (
	StatusPending   Status = "pending"
	StatusScheduled Status = "scheduled"
	StatusQueued    Status = "queued"
	StatusSending   Status = "sending"
	StatusSent      Status = "sent"
	StatusRetrying  Status = "retrying"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Statuses lists every status in lifecycle order.
var Statuses = []Status{
	StatusPending, StatusScheduled, StatusQueued, StatusSending,
	StatusSent, StatusRetrying, StatusFailed, StatusCancelled,
}

// transitions is the single source of truth for FR-012. Any pair not listed is illegal.
var transitions = map[Status][]Status{
	StatusPending:   {StatusQueued, StatusCancelled},
	StatusScheduled: {StatusQueued, StatusCancelled},
	StatusQueued:    {StatusSending, StatusCancelled},
	StatusSending:   {StatusSent, StatusRetrying, StatusFailed},
	StatusRetrying:  {StatusQueued, StatusCancelled},
	// sent, failed and cancelled are terminal.
}

// Allowed-from sets used by guarded updates (UPDATE … WHERE status = ANY($from)).
var (
	// CancellableFrom are the states a cancel may move out of (FR-013).
	CancellableFrom = []Status{StatusPending, StatusScheduled, StatusQueued, StatusRetrying}
	// DueFrom are the states the outbox relay picks up once next_attempt_at has passed.
	DueFrom = []Status{StatusPending, StatusScheduled, StatusRetrying}
	// SendableFrom is the only state a consumer may claim for sending (atomic cancel check, FR-014).
	SendableFrom = []Status{StatusQueued}
	// InFlight are non-terminal states owned by the broker or a consumer.
	InFlight = []Status{StatusQueued, StatusSending}
)

// ParseStatus accepts one of the eight lifecycle states.
func ParseStatus(s string) (Status, error) {
	for _, st := range Statuses {
		if string(st) == s {
			return st, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidStatus, s)
}

// CanTransition reports whether from → to is allowed by FR-012.
func CanTransition(from, to Status) bool {
	for _, t := range transitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// AllowedFrom returns every state that may transition into to. Repositories use it as the guard.
func AllowedFrom(to Status) []Status {
	var out []Status
	for _, from := range Statuses {
		if CanTransition(from, to) {
			out = append(out, from)
		}
	}
	return out
}

// IsTerminal reports whether no transition leaves s.
func (s Status) IsTerminal() bool { return len(transitions[s]) == 0 }

// In reports whether s is one of set.
func (s Status) In(set []Status) bool {
	for _, x := range set {
		if x == s {
			return true
		}
	}
	return false
}

func (s Status) String() string { return string(s) }
