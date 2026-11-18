package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidPriority is returned for an unknown priority value.
var ErrInvalidPriority = errors.New("invalid priority")

// Priority orders delivery under a backlog (data-model.md notifications.priority). Its number is
// also the AMQP message priority, so higher means sooner.
type Priority int16

const (
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// ParsePriority accepts "high", "normal" or "low"; an empty string means normal.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal", "":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	default:
		return 0, fmt.Errorf("%w: %q (want high, normal or low)", ErrInvalidPriority, s)
	}
}

// PriorityFromInt converts a stored value back, rejecting anything outside 1..3.
func PriorityFromInt(n int16) (Priority, error) {
	p := Priority(n)
	if p < PriorityLow || p > PriorityHigh {
		return 0, fmt.Errorf("%w: %d", ErrInvalidPriority, n)
	}
	return p, nil
}

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return fmt.Sprintf("priority(%d)", int16(p))
	}
}
