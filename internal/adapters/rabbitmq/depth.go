package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// DepthInspector reports ready-message counts per queue via passive declares, which never
// create or change a queue (research R16).
type DepthInspector struct {
	conn   *amqp.Connection
	queues []string
}

var _ ports.QueueInspector = (*DepthInspector)(nil)

// NewDepthInspector inspects the channel work queues and the DLQ.
func NewDepthInspector(conn *amqp.Connection) *DepthInspector {
	qs := make([]string, 0, len(domain.Channels)+1)
	for _, ch := range domain.Channels {
		qs = append(qs, QueueName(ch))
	}
	return &DepthInspector{conn: conn, queues: append(qs, DLQ)}
}

// Depths returns the ready-message count of every known queue. A failed passive declare closes
// the AMQP channel, so each queue gets its own short-lived channel.
func (d *DepthInspector) Depths(ctx context.Context) (map[string]int, error) {
	out := make(map[string]int, len(d.queues))
	var errs []error
	for _, q := range d.queues {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("queue depths: %w", err)
		}
		n, err := d.depth(q)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out[q] = n
	}
	return out, errors.Join(errs...)
}

func (d *DepthInspector) depth(queue string) (int, error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", queue, err)
	}
	return q.Messages, nil
}
