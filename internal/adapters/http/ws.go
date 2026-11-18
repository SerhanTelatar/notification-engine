package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/ws"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
)

// WebSocket timings (contracts/events.md §3).
const (
	DefaultPingInterval = 30 * time.Second
	DefaultPongTimeout  = 10 * time.Second
	wsWriteTimeout      = 10 * time.Second
	wsReadLimit         = 4096 // clients only send control frames; anything else is ignored
)

// WSDeps are the /ws handler's collaborators.
type WSDeps struct {
	Hub          *ws.Hub
	Query        *app.Query   // notification lookup + snapshot
	Batches      *app.Batcher // batch lookup + snapshot
	Log          *slog.Logger
	Metrics      *metrics.API  // optional: ws_connections
	PingInterval time.Duration // 0 = DefaultPingInterval
	PongTimeout  time.Duration // 0 = DefaultPongTimeout
}

// MountWS mounts GET /ws (US9). It's outside the generated strict server because it hijacks the
// connection (plan.md Complexity Tracking).
func MountWS(r chi.Router, d WSDeps) {
	if d.PingInterval <= 0 {
		d.PingInterval = DefaultPingInterval
	}
	if d.PongTimeout <= 0 {
		d.PongTimeout = DefaultPongTimeout
	}
	r.Get("/ws", (&wsHandler{d: d}).serve)
}

type wsHandler struct{ d WSDeps }

type snapshotItem struct {
	NotificationID string    `json:"notification_id"`
	Status         string    `json:"status"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type snapshotMessage struct {
	Type          string         `json:"type"`
	Notifications []snapshotItem `json:"notifications"`
}

// serve validates the subscription and checks the id exists before upgrading, then streams a
// snapshot followed by every status change.
func (h *wsHandler) serve(w http.ResponseWriter, r *http.Request) {
	errorOut := responseErrorHandler(h.d.Log)
	notificationID, batchID, err := wsParams(r)
	if err != nil {
		errorOut(w, r, err)
		return
	}

	// Subscribe before reading the snapshot, so a change committed in between is still seen
	// (possibly as well as in the snapshot, which is harmless).
	sub := h.d.Hub.Subscribe(notificationID, batchID)
	defer sub.Close()
	snapshot, err := h.snapshot(r.Context(), notificationID, batchID)
	if err != nil {
		errorOut(w, r, err)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		h.d.Log.WarnContext(r.Context(), "websocket upgrade failed", "error", err)
		return // Accept already wrote the HTTP error
	}
	defer func() { _ = c.CloseNow() }()
	if h.d.Metrics != nil {
		h.d.Metrics.WSConnections.Inc()
		defer h.d.Metrics.WSConnections.Dec()
	}

	// The connection outlives the request timeout: detach from it, and end when the client goes.
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	c.SetReadLimit(wsReadLimit)
	go func() { // read (and ignore) client frames so pings/pongs/close are processed
		defer cancel()
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}()

	if err := h.write(ctx, c, snapshot); err != nil {
		return
	}
	ping := time.NewTicker(h.d.PingInterval)
	defer ping.Stop()
	for {
		select {
		case msg := <-sub.Messages():
			if err := h.writeRaw(ctx, c, msg); err != nil {
				return
			}
		case <-ping.C:
			pctx, pcancel := context.WithTimeout(ctx, h.d.PongTimeout)
			err := c.Ping(pctx)
			pcancel()
			if err != nil {
				_ = c.Close(websocket.StatusPolicyViolation, "pong timeout")
				return
			}
		case <-sub.Done():
			_ = c.Close(websocket.StatusCode(sub.CloseCode()), closeReason(sub.CloseCode()))
			return
		case <-ctx.Done():
			return // the client went away
		}
	}
}

// wsParams accepts exactly one well-formed id (contracts/events.md §3).
func wsParams(r *http.Request) (notificationID, batchID string, err error) {
	q := r.URL.Query()
	notificationID, batchID = q.Get("notification_id"), q.Get("batch_id")
	switch {
	case notificationID == "" && batchID == "":
		return "", "", domain.NewFieldError("notification_id", "exactly one of notification_id or batch_id is required")
	case notificationID != "" && batchID != "":
		return "", "", domain.NewFieldError("batch_id", "give notification_id or batch_id, not both")
	}
	field, id := "notification_id", notificationID
	if batchID != "" {
		field, id = "batch_id", batchID
	}
	if _, perr := uuid.Parse(id); perr != nil {
		return "", "", domain.NewFieldError(field, "must be a UUID")
	}
	return notificationID, batchID, nil
}

// snapshot returns the current status of the subscribed notification or of every batch item.
// An unknown id is domain.ErrNotFound (404).
func (h *wsHandler) snapshot(ctx context.Context, notificationID, batchID string) (snapshotMessage, error) {
	msg := snapshotMessage{Type: "snapshot", Notifications: []snapshotItem{}}
	if notificationID != "" {
		d, err := h.d.Query.GetNotification(ctx, notificationID)
		if err != nil {
			return msg, err //nolint:wrapcheck // domain errors are mapped to the contract
		}
		msg.Notifications = append(msg.Notifications, toSnapshot(d.Notification))
		return msg, nil
	}
	b, err := h.d.Batches.GetBatch(ctx, batchID)
	if err != nil {
		return msg, err //nolint:wrapcheck // domain errors are mapped to the contract
	}
	for i := range b.Items {
		msg.Notifications = append(msg.Notifications, toSnapshot(&b.Items[i]))
	}
	return msg, nil
}

func toSnapshot(n *domain.Notification) snapshotItem {
	return snapshotItem{NotificationID: n.ID, Status: string(n.Status), UpdatedAt: n.UpdatedAt}
}

func (h *wsHandler) write(ctx context.Context, c *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return errors.New("encode websocket message")
	}
	return h.writeRaw(ctx, c, b)
}

func (h *wsHandler) writeRaw(ctx context.Context, c *websocket.Conn, b []byte) error {
	wctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, b) //nolint:wrapcheck // the caller only stops on error
}

func closeReason(code int) string {
	switch code {
	case ws.ClosePolicyViolation:
		return "send buffer full; reconnect for a fresh snapshot"
	case ws.CloseGoingAway:
		return "server shutting down"
	default:
		return ""
	}
}
