// Package rabbitmq is the AMQP adapter: topology, a confirming publisher and per-channel
// consumers (contracts/events.md §1, ADR 0001).
package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// Topology names (contracts/events.md §1).
const (
	Exchange    = "notifications"     // direct, routing key = channel
	DLXExchange = "notifications.dlx" // fanout
	DLQ         = "notifications.dlq"
	MaxPriority = 3 // priorities 1..3 = domain.Priority
)

// QueueName is the work queue for one channel, e.g. notifications.sms.
func QueueName(ch domain.Channel) string { return Exchange + "." + string(ch) }

// Dial connects with a heartbeat and a connection name visible in the management UI.
func Dial(url, name string) (*amqp.Connection, error) {
	props := amqp.NewConnectionProperties()
	props.SetClientConnectionName(name)
	conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Properties: props, Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	return conn, nil
}

// DeclareTopology declares the exchanges, queues and bindings. Every declaration is idempotent,
// so each worker runs it on startup.
func DeclareTopology(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.ExchangeDeclare(Exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", Exchange, err)
	}
	if err := ch.ExchangeDeclare(DLXExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", DLXExchange, err)
	}
	if _, err := ch.QueueDeclare(DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", DLQ, err)
	}
	if err := ch.QueueBind(DLQ, "", DLXExchange, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", DLQ, err)
	}
	for _, c := range domain.Channels {
		q := QueueName(c)
		args := amqp.Table{
			"x-max-priority":         int32(MaxPriority),
			"x-dead-letter-exchange": DLXExchange,
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", q, err)
		}
		if err := ch.QueueBind(q, string(c), Exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", q, err)
		}
	}
	return nil
}
