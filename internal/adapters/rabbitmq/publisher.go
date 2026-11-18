package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// DefaultPublishTimeout bounds one Publish call, confirms included.
const DefaultPublishTimeout = 10 * time.Second

// returnBuffer must exceed the largest batch: basic.return frames for one batch are buffered and
// drained after its confirms.
const returnBuffer = 4096

// ErrNotConfirmed means the broker nacked or returned a message, so the batch wasn't published.
var ErrNotConfirmed = errors.New("message not confirmed by broker")

// Publisher implements ports.Publisher with publisher confirms (research R4). Publish returns nil
// only when every message in the batch was routed and confirmed; any failure rolls back the
// caller's relay transaction. A broken channel is reopened on the next call; a broken connection
// surfaces as an error, and the worker restarts.
type Publisher struct {
	conn    *amqp.Connection
	timeout time.Duration
	opts    options

	mu      sync.Mutex // one batch at a time per channel; confirms are per channel
	ch      *amqp.Channel
	returns chan amqp.Return
}

var _ ports.Publisher = (*Publisher)(nil)

// NewPublisher returns a publisher on conn; timeout <= 0 means DefaultPublishTimeout.
func NewPublisher(conn *amqp.Connection, timeout time.Duration, opts ...Option) *Publisher {
	if timeout <= 0 {
		timeout = DefaultPublishTimeout
	}
	return &Publisher{conn: conn, timeout: timeout, opts: applyOptions(opts)}
}

// Publish sends msgs persistently with their priority and waits for every confirm.
func (p *Publisher) Publish(ctx context.Context, msgs []ports.Message) (err error) {
	if len(msgs) == 0 {
		return nil
	}
	if len(msgs) > returnBuffer {
		return fmt.Errorf("batch of %d exceeds the publisher limit of %d", len(msgs), returnBuffer)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	ch, err := p.channel()
	if err != nil {
		return err
	}
	confirms := make([]*amqp.DeferredConfirmation, 0, len(msgs))
	var spans []trace.Span
	defer func() { endSpans(spans, err) }() // each span covers its message's publish and confirm
	for _, m := range msgs {
		body, err := encode(m)
		if err != nil {
			return err
		}
		headers := amqp.Table{HeaderCorrelationID: m.CorrelationID}
		span, traceParent := p.opts.publishSpan(ctx, m)
		if span != nil {
			spans = append(spans, span)
		}
		if traceParent != "" {
			headers[tracing.HeaderTraceParent] = traceParent
		}
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, Exchange, string(m.Channel),
			true,  // mandatory: an unroutable message comes back as basic.return
			false, // immediate is unsupported by RabbitMQ
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Priority:     uint8(m.Priority), //nolint:gosec // 1..3
				ContentType:  "application/json",
				MessageId:    m.NotificationID,
				Timestamp:    m.EnqueuedAt,
				Headers:      headers,
				Body:         body,
			})
		if err != nil {
			p.reset()
			return fmt.Errorf("publish %s: %w", m.NotificationID, err)
		}
		confirms = append(confirms, dc)
	}
	for i, dc := range confirms {
		acked, err := dc.WaitContext(ctx)
		if err != nil {
			p.reset()
			return fmt.Errorf("await confirm for %s: %w", msgs[i].NotificationID, err)
		}
		if !acked {
			return fmt.Errorf("%s: nacked: %w", msgs[i].NotificationID, ErrNotConfirmed)
		}
	}
	// basic.return is delivered before the ack of the same message, so every return for this
	// batch is already buffered.
	if returned := p.drainReturns(); len(returned) > 0 {
		return fmt.Errorf("%d message(s) unroutable (first %s): %w", len(returned), returned[0], ErrNotConfirmed)
	}
	return nil
}

// channel returns the confirm-mode channel, (re)opening it if needed.
func (p *Publisher) channel() (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	p.returns = ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	p.ch = ch
	return ch, nil
}

func (p *Publisher) drainReturns() []string {
	var ids []string
	for {
		select {
		case r, ok := <-p.returns:
			if !ok {
				return ids
			}
			ids = append(ids, r.MessageId)
		default:
			return ids
		}
	}
}

// reset drops a broken channel; the next Publish opens a new one.
func (p *Publisher) reset() {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	p.ch = nil
}

// Close releases the channel.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reset()
}
