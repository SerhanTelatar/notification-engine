package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// StatusChannel is the pub/sub channel for status events (contracts/events.md §2).
const StatusChannel = "notification-status"

// publishTimeout bounds a publish: status events are best-effort and must never hold up a commit
// path (research R11).
const publishTimeout = 500 * time.Millisecond

// StatusBus publishes status events to Redis and subscribes to them (US9). Delivery is
// best-effort: a lost event is never replayed; subscribers recover with the snapshot on connect.
type StatusBus struct {
	rdb goredis.UniversalClient
	log *slog.Logger
}

var _ ports.StatusBroadcaster = (*StatusBus)(nil)

// NewStatusBus returns a bus on rdb.
func NewStatusBus(rdb goredis.UniversalClient, log *slog.Logger) *StatusBus {
	return &StatusBus{rdb: rdb, log: log}
}

// statusPayload is the wire format (contracts/events.md §2).
type statusPayload struct {
	NotificationID string    `json:"notification_id"`
	BatchID        *string   `json:"batch_id"`
	From           *string   `json:"from"`
	To             string    `json:"to"`
	Attempts       int       `json:"attempts"`
	At             time.Time `json:"at"`
	CorrelationID  string    `json:"correlation_id"`
}

// Broadcast publishes ev. It's detached from the caller's cancellation (the commit already
// happened) but bounded by publishTimeout.
func (b *StatusBus) Broadcast(ctx context.Context, ev ports.StatusEvent) error {
	p := statusPayload{NotificationID: ev.NotificationID, BatchID: ev.BatchID, To: string(ev.To),
		Attempts: ev.Attempts, At: ev.At.UTC(), CorrelationID: ev.CorrelationID}
	if ev.From != nil {
		from := string(*ev.From)
		p.From = &from
	}
	msg, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode status event: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if err := b.rdb.Publish(ctx, StatusChannel, msg).Err(); err != nil {
		return fmt.Errorf("publish status event: %w", err)
	}
	return nil
}

// Run subscribes to status events and calls deliver for each one until ctx is cancelled. The
// go-redis subscription reconnects by itself after a Redis outage; events published meanwhile
// are lost (best-effort). Malformed messages are logged and skipped.
func (b *StatusBus) Run(ctx context.Context, deliver func(ports.StatusEvent)) error {
	sub := b.rdb.Subscribe(ctx, StatusChannel)
	defer func() { _ = sub.Close() }()
	msgs := sub.Channel(goredis.WithChannelHealthCheckInterval(10 * time.Second))
	b.log.InfoContext(ctx, "status subscription started", "channel", StatusChannel)
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-msgs:
			if !ok {
				return nil
			}
			ev, err := decodeStatus(m.Payload)
			if err != nil {
				b.log.WarnContext(ctx, "malformed status event skipped", "error", err)
				continue
			}
			deliver(ev)
		}
	}
}

func decodeStatus(payload string) (ports.StatusEvent, error) {
	var p statusPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return ports.StatusEvent{}, fmt.Errorf("decode status event: %w", err)
	}
	if p.NotificationID == "" || p.To == "" {
		return ports.StatusEvent{}, fmt.Errorf("status event without notification_id or to")
	}
	ev := ports.StatusEvent{NotificationID: p.NotificationID, BatchID: p.BatchID, To: domain.Status(p.To),
		Attempts: p.Attempts, At: p.At, CorrelationID: p.CorrelationID}
	if p.From != nil {
		from := domain.Status(*p.From)
		ev.From = &from
	}
	return ev, nil
}
