// Package app holds the use cases. It depends only on internal/domain and internal/ports
// (Constitution II); adapters are injected through the ports.
package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// Log attribute keys. On request and message paths the platform logger also adds them from ctx;
// app code sets them explicitly when it handles rows that aren't the request's own (relay, reaper).
const (
	attrCorrelationID  = "correlation_id"
	attrNotificationID = "notification_id"
)

// broadcast publishes a committed status change. It's best-effort: a failure is logged and never
// fails the use case (research R11).
func broadcast(ctx context.Context, log *slog.Logger, b ports.StatusBroadcaster, n *domain.Notification, from *domain.Status, at time.Time) {
	ev := ports.StatusEvent{
		NotificationID: n.ID, BatchID: n.BatchID, From: from, To: n.Status,
		Attempts: n.Attempts, At: at, CorrelationID: n.CorrelationID,
	}
	if err := b.Broadcast(ctx, ev); err != nil {
		log.WarnContext(ctx, "status broadcast failed", attrNotificationID, n.ID,
			attrCorrelationID, n.CorrelationID, "to", n.Status, "error", err)
	}
}

func statusPtr(s domain.Status) *domain.Status { return &s }
