// Package ws fans status events out to WebSocket subscribers (US9, contracts/events.md §3). The
// hub is transport-agnostic: a Subscription is a bounded queue of encoded messages plus a close
// code, which the HTTP handler drains onto a websocket.Conn.
package ws

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// DefaultBuffer is each client's send buffer (contracts/events.md §3).
const DefaultBuffer = 64

// WebSocket close codes the hub decides on.
const (
	CloseNormal          = 1000
	CloseGoingAway       = 1001 // server shutdown
	ClosePolicyViolation = 1008 // the client's buffer overflowed: it should reconnect for a fresh snapshot
)

// Hub indexes subscriptions by notification id and by batch id.
type Hub struct {
	buf     int
	mu      sync.Mutex
	byNotif map[string]map[*Subscription]struct{}
	byBatch map[string]map[*Subscription]struct{}
	n       int
	closed  bool
}

// NewHub returns a hub whose clients buffer up to buf messages (0 = DefaultBuffer).
func NewHub(buf int) *Hub {
	if buf <= 0 {
		buf = DefaultBuffer
	}
	return &Hub{buf: buf, byNotif: map[string]map[*Subscription]struct{}{}, byBatch: map[string]map[*Subscription]struct{}{}}
}

// Subscription is one client's interest in a notification or a batch.
type Subscription struct {
	hub     *Hub
	key     string
	byBatch bool
	msgs    chan []byte
	done    chan struct{}
	once    sync.Once
	code    atomic.Int32
}

// Messages yields encoded status messages. It's never closed; watch Done.
func (s *Subscription) Messages() <-chan []byte { return s.msgs }

// Done is closed when the subscription ends: Close, overflow or hub shutdown.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// CloseCode is the WebSocket close code the client should receive once Done is closed.
func (s *Subscription) CloseCode() int { return int(s.code.Load()) }

// Close ends the subscription (the client went away). It's idempotent.
func (s *Subscription) Close() {
	s.hub.remove(s)
	s.finish(CloseNormal)
}

func (s *Subscription) finish(code int) {
	s.once.Do(func() {
		s.code.Store(int32(code)) //nolint:gosec // WebSocket close codes fit in int32
		close(s.done)
	})
}

// Subscribe registers interest in a notification (notificationID) or a batch (batchID); exactly
// one should be set. After Shutdown it returns an already-closed subscription.
func (h *Hub) Subscribe(notificationID, batchID string) *Subscription {
	s := &Subscription{hub: h, msgs: make(chan []byte, h.buf), done: make(chan struct{})}
	index := h.byNotif
	s.key = notificationID
	if batchID != "" {
		index, s.key, s.byBatch = h.byBatch, batchID, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		s.finish(CloseGoingAway)
		return s
	}
	if index[s.key] == nil {
		index[s.key] = map[*Subscription]struct{}{}
	}
	index[s.key][s] = struct{}{}
	h.n++
	return s
}

// Len is the number of live subscriptions.
func (h *Hub) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// statusMessage is what clients receive per change (contracts/events.md §3).
type statusMessage struct {
	Type           string    `json:"type"`
	NotificationID string    `json:"notification_id"`
	BatchID        *string   `json:"batch_id"`
	From           *string   `json:"from"`
	To             string    `json:"to"`
	Attempts       int       `json:"attempts"`
	At             time.Time `json:"at"`
}

// Publish delivers ev to the notification's and its batch's subscribers without blocking. A
// subscriber whose buffer is full is dropped with 1008 so one slow client can't stall the others.
func (h *Hub) Publish(ev ports.StatusEvent) {
	m := statusMessage{Type: "status", NotificationID: ev.NotificationID, BatchID: ev.BatchID,
		To: string(ev.To), Attempts: ev.Attempts, At: ev.At.UTC()}
	if ev.From != nil {
		from := string(*ev.From)
		m.From = &from
	}
	msg, err := json.Marshal(m)
	if err != nil {
		return // only plain strings and numbers: can't happen
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	var targets []*Subscription
	for s := range h.byNotif[ev.NotificationID] {
		targets = append(targets, s)
	}
	if ev.BatchID != nil {
		for s := range h.byBatch[*ev.BatchID] {
			targets = append(targets, s)
		}
	}
	for _, s := range targets {
		select {
		case s.msgs <- msg:
		default:
			h.removeLocked(s)
			s.finish(ClosePolicyViolation)
		}
	}
}

// Shutdown closes every subscription with 1001; later subscriptions are closed immediately.
func (h *Hub) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, index := range []map[string]map[*Subscription]struct{}{h.byNotif, h.byBatch} {
		for key, subs := range index {
			for s := range subs {
				s.finish(CloseGoingAway)
			}
			delete(index, key)
		}
	}
	h.n = 0
}

func (h *Hub) remove(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(s)
}

func (h *Hub) removeLocked(s *Subscription) {
	index := h.byNotif
	if s.byBatch {
		index = h.byBatch
	}
	subs, ok := index[s.key]
	if !ok {
		return
	}
	if _, ok := subs[s]; !ok {
		return
	}
	delete(subs, s)
	if len(subs) == 0 {
		delete(index, s.key)
	}
	h.n--
}
