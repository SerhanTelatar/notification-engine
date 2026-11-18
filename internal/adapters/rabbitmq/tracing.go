package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/SerhanTelatar/notification-engine/internal/platform/tracing"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Option configures a Publisher or a Consumer.
type Option func(*options)

type options struct{ tracing *tracing.Tracing }

// WithTracing records producer spans when publishing and consumer spans when delivering, and
// carries the W3C traceparent in the message headers (bonus, T117).
func WithTracing(t *tracing.Tracing) Option { return func(o *options) { o.tracing = t } }

func applyOptions(opts []Option) options {
	var o options
	for _, f := range opts {
		f(&o)
	}
	return o
}

// publishSpan starts the producer span for m as a child of the trace stored with the row, and
// returns the traceparent to put on the message. With tracing off the stored traceparent is
// passed through unchanged, so a traced API and an untraced worker still link up.
func (o options) publishSpan(ctx context.Context, m ports.Message) (trace.Span, string) {
	if !o.tracing.Enabled() {
		return nil, m.TraceParent
	}
	ctx = tracing.ContextWithTraceParent(ctx, m.TraceParent)
	ctx, span := o.tracing.Tracer().Start(ctx, "publish "+QueueName(m.Channel),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", QueueName(m.Channel)),
			attribute.String("notification_id", m.NotificationID),
			attribute.String("correlation_id", m.CorrelationID),
		))
	return span, tracing.TraceParent(ctx)
}

// deliverSpan continues the message's trace with a consumer span; it returns ctx unchanged and a
// nil span when tracing is off (the remote parent is still put in ctx for the provider call).
func (o options) deliverSpan(ctx context.Context, ch string, d amqp.Delivery) (context.Context, trace.Span) {
	tp, _ := d.Headers[tracing.HeaderTraceParent].(string)
	ctx = tracing.ContextWithTraceParent(ctx, tp)
	if !o.tracing.Enabled() {
		return ctx, nil
	}
	return o.tracing.Tracer().Start(ctx, "deliver "+ch, //nolint:spancheck // ended by the caller
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", d.RoutingKey),
			attribute.String("messaging.message.id", d.MessageId),
			attribute.Bool("messaging.rabbitmq.redelivered", d.Redelivered),
		))
}

// endSpans ends spans, marking them failed when err is set.
func endSpans(spans []trace.Span, err error) {
	for _, s := range spans {
		if err != nil {
			s.RecordError(err)
			s.SetStatus(codes.Error, "publish failed")
		}
		s.End()
	}
}
