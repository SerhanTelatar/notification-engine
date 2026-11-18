package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Consumer drains one channel's queue with a bounded number of concurrent handlers (research
// R6). Prefetch equals concurrency, so at most that many messages are off the queue per worker,
// which is the priority bound SC-007 allows.
type Consumer struct {
	conn        *amqp.Connection
	channel     domain.Channel
	concurrency int
	handle      ports.MessageHandler
	log         *slog.Logger
}

// NewConsumer returns a consumer for ch's queue.
func NewConsumer(conn *amqp.Connection, ch domain.Channel, concurrency int, handle ports.MessageHandler, log *slog.Logger) *Consumer {
	return &Consumer{conn: conn, channel: ch, concurrency: max(1, concurrency), handle: handle,
		log: log.With("component", "consumer", "channel", string(ch))}
}

// Run consumes until ctx is cancelled, then stops taking deliveries and waits for in-flight
// handlers (research R17). Unacked messages return to the queue when the channel closes. A broker
// failure returns an error, so the worker exits and restarts.
func (c *Consumer) Run(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.concurrency, 0, false); err != nil {
		return fmt.Errorf("set prefetch: %w", err)
	}
	tag := fmt.Sprintf("worker-%s-%s", c.channel, uuid.NewString()[:8])
	deliveries, err := ch.Consume(QueueName(c.channel), tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", QueueName(c.channel), err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	var wg sync.WaitGroup
	for range c.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range deliveries {
				c.handleDelivery(ctx, d)
			}
		}()
	}
	c.log.InfoContext(ctx, "consumer started", "queue", QueueName(c.channel), "concurrency", c.concurrency)

	var runErr error
	select {
	case <-ctx.Done():
		// Stop new deliveries; the deliveries channel closes once the broker confirms, and the
		// handlers finish whatever they already hold.
		if err := ch.Cancel(tag, false); err != nil {
			c.log.WarnContext(ctx, "consumer cancel failed", "error", err)
		}
	case amqpErr := <-closed:
		runErr = fmt.Errorf("consumer channel for %s closed: %w", c.channel, closeReason(amqpErr))
	}
	wg.Wait()
	c.log.InfoContext(context.WithoutCancel(ctx), "consumer stopped")
	return runErr
}

func (c *Consumer) handleDelivery(ctx context.Context, d amqp.Delivery) {
	correlationID, _ := d.Headers[HeaderCorrelationID].(string)
	ctx = logger.WithCorrelationID(ctx, correlationID)

	msg, err := decode(d.Body, correlationID)
	if err == nil && msg.Channel != c.channel {
		err = fmt.Errorf("message for %s arrived on the %s queue", msg.Channel, c.channel)
	}
	if err != nil {
		c.log.ErrorContext(ctx, "poison message dead-lettered", "message_id", d.MessageId, "error", err)
		c.settle(ctx, d, ports.DecisionDeadLetter)
		return
	}
	ctx = logger.WithNotificationID(ctx, msg.NotificationID)

	decision := c.safeHandle(ctx, msg)
	c.settle(ctx, d, decision)
}

// safeHandle runs the handler; a panic is a bug, so the message is dead-lettered instead of being
// redelivered forever.
func (c *Consumer) safeHandle(ctx context.Context, msg ports.Message) (decision ports.Decision) {
	defer func() {
		if p := recover(); p != nil {
			c.log.ErrorContext(ctx, "handler panic, message dead-lettered", "panic", p)
			decision = ports.DecisionDeadLetter
		}
	}()
	return c.handle(ctx, msg)
}

func (c *Consumer) settle(ctx context.Context, d amqp.Delivery, decision ports.Decision) {
	var err error
	switch decision {
	case ports.DecisionAck:
		err = d.Ack(false)
	case ports.DecisionRequeue:
		err = d.Nack(false, true)
	case ports.DecisionDeadLetter:
		err = d.Nack(false, false) // routed by the queue's DLX to notifications.dlq
	}
	if err != nil {
		// The channel is gone: the broker requeues the message itself.
		c.log.WarnContext(ctx, "settling delivery failed", "decision", decision.String(), "error", err)
	}
}

// closeReason turns a channel-close notification into an error; a nil *amqp.Error means the
// channel closed without a broker-reported reason.
func closeReason(e *amqp.Error) error {
	if e == nil {
		return errors.New("closed without a reason")
	}
	return e
}
