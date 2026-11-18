package rabbitmq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// HeaderCorrelationID carries the correlation id end to end (FR-032).
const HeaderCorrelationID = "x-correlation-id"

// wireMessage is the body on the queue (contracts/events.md §1). It's deliberately minimal:
// the database row is authoritative, so content never travels through the broker.
type wireMessage struct {
	NotificationID string    `json:"notification_id"`
	Channel        string    `json:"channel"`
	Priority       int16     `json:"priority"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
}

func encode(m ports.Message) ([]byte, error) {
	b, err := json.Marshal(wireMessage{
		NotificationID: m.NotificationID, Channel: string(m.Channel),
		Priority: int16(m.Priority), EnqueuedAt: m.EnqueuedAt.UTC(),
	})
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}
	return b, nil
}

// decode parses and validates a body; a failure means a poison message.
func decode(body []byte, correlationID string) (ports.Message, error) {
	var w wireMessage
	if err := json.Unmarshal(body, &w); err != nil {
		return ports.Message{}, fmt.Errorf("decode message: %w", err)
	}
	if w.NotificationID == "" {
		return ports.Message{}, fmt.Errorf("decode message: missing notification_id")
	}
	ch, err := domain.ParseChannel(w.Channel)
	if err != nil {
		return ports.Message{}, fmt.Errorf("decode message: %w", err)
	}
	p, err := domain.PriorityFromInt(w.Priority)
	if err != nil {
		return ports.Message{}, fmt.Errorf("decode message: %w", err)
	}
	return ports.Message{NotificationID: w.NotificationID, Channel: ch, Priority: p,
		CorrelationID: correlationID, EnqueuedAt: w.EnqueuedAt}, nil
}
