package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// RelayDeps are the Relay's collaborators.
type RelayDeps struct {
	Repo        ports.NotificationRepository
	Publisher   ports.Publisher
	Events      ports.StatusBroadcaster
	Metrics     ports.MetricsRecorder
	Clock       ports.Clock
	IDs         ports.IDGenerator // tick ids for logs
	Log         *slog.Logger
	MaxInflight int // RELAY_MAX_INFLIGHT_PER_CHANNEL
	BatchSize   int // RELAY_BATCH_SIZE
}

// Relay is the transactional-outbox relay (research R4, ADR 0002). Each tick it moves due rows to
// queued and publishes them, channel by channel, while each channel stays under its in-flight
// cap.
type Relay struct{ d RelayDeps }

// NewRelay returns the relay use case.
func NewRelay(d RelayDeps) *Relay { return &Relay{d: d} }

// Quota is the backpressure rule: publish at most min(batch, maxInflight − queued) rows, and never
// a negative number (research R4).
func Quota(batch, maxInflight, queued int) int {
	return max(0, min(batch, maxInflight-queued))
}

// Tick runs one relay pass over every channel. A failure on one channel doesn't stop the others;
// all failures are returned joined.
func (r *Relay) Tick(ctx context.Context) error {
	tickID := r.d.IDs.NewID()
	log := r.d.Log.With("tick_id", tickID)
	var errs []error
	for _, ch := range domain.Channels {
		if err := r.relayChannel(ctx, log, ch); err != nil {
			log.ErrorContext(ctx, "relay pass failed", "channel", ch, "error", err)
			errs = append(errs, fmt.Errorf("relay %s: %w", ch, err))
		}
	}
	return errors.Join(errs...)
}

func (r *Relay) relayChannel(ctx context.Context, log *slog.Logger, ch domain.Channel) error {
	now := r.d.Clock.Now()
	var picked []domain.Notification
	publish := func(ctx context.Context, rows []domain.Notification) error {
		msgs := make([]ports.Message, len(rows))
		for i, n := range rows {
			msgs[i] = ports.Message{NotificationID: n.ID, Channel: n.Channel, Priority: n.Priority,
				CorrelationID: n.CorrelationID, EnqueuedAt: now}
		}
		if err := r.d.Publisher.Publish(ctx, msgs); err != nil {
			for _, n := range rows {
				log.WarnContext(ctx, "publish failed, notification stays due",
					attrNotificationID, n.ID, attrCorrelationID, n.CorrelationID, "channel", ch)
			}
			return err //nolint:wrapcheck // wrapped by the repository with the relay context
		}
		picked = rows
		return nil
	}
	quota := func(queued int) int { return Quota(r.d.BatchSize, r.d.MaxInflight, queued) }

	res, err := r.d.Repo.RelayTx(ctx, ch, now, quota, publish)
	if err != nil {
		return err //nolint:wrapcheck // wrapped by Tick with the channel
	}
	switch res.Skipped {
	case "backpressure":
		r.d.Metrics.RelaySkipped(ch)
		log.DebugContext(ctx, "relay backpressure: channel at its in-flight cap", "channel", ch, "max_inflight", r.d.MaxInflight)
		return nil
	case "locked":
		log.DebugContext(ctx, "relay: channel held by another relay", "channel", ch)
		return nil
	}
	if res.Published == 0 {
		return nil
	}
	r.d.Metrics.RelayPublished(ch, res.Published)
	for i := range picked {
		n := &picked[i]
		from := n.Status // pending, scheduled or retrying
		n.Status = domain.StatusQueued
		log.DebugContext(ctx, "notification queued", attrNotificationID, n.ID, attrCorrelationID, n.CorrelationID, "from", from)
		broadcast(ctx, log, r.d.Events, n, &from, now)
	}
	log.InfoContext(ctx, "relay published", "channel", ch, "count", res.Published)
	return nil
}
