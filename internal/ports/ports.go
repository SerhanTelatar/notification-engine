// Package ports declares the interfaces the application layer depends on (Constitution II).
// Adapters implement them; use cases never import an adapter. Every I/O method takes a
// context.Context first.
package ports

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// ---------------------------------------------------------------------------------------------
// Time and identity
// ---------------------------------------------------------------------------------------------

// Clock abstracts time so tests control it.
type Clock interface {
	Now() time.Time
}

// IDGenerator produces new entity ids (UUIDv7, data-model.md notifications.id).
type IDGenerator interface {
	NewID() string
}

// ---------------------------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------------------------

// ListFilter selects notifications for keyset pagination (FR-011). From is inclusive, To
// exclusive; After is the decoded cursor of the previous page.
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	From    *time.Time
	To      *time.Time
	After   *Cursor
	Limit   int
}

// Cursor is the keyset position (created_at, id), newest first.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// RelayResult reports one channel's relay pass.
type RelayResult struct {
	Published int
	// Skipped is set when the pass published nothing on purpose: "locked" (another relay holds
	// the channel) or "backpressure" (queued count is at the cap).
	Skipped string
}

// PublishFunc publishes rows inside the relay transaction. A non-nil error rolls it back. Relay
// rows carry their status from before the relay (pending, scheduled or retrying).
type PublishFunc func(ctx context.Context, rows []domain.Notification) error

// QuotaFunc turns a channel's current queued count into how many due rows to pick now. The
// application owns the backpressure rule (research R4); <= 0 means skip the channel.
type QuotaFunc func(queued int) int

// ReapDecision decides how a stale sending row is recovered (research R5): retrying with an
// abandoned attempt, or failed when attempts are exhausted. The repository applies it in the
// same transaction that locked the row.
type ReapDecision func(n domain.Notification) DeliveryOutcome

// DeliveryOutcome is the result of one provider call, applied atomically with its attempt row.
type DeliveryOutcome struct {
	NotificationID    string
	Attempt           domain.Attempt
	To                domain.Status // sent, retrying or failed
	NextAttemptAt     time.Time     // for retrying
	ProviderMessageID *string       // for sent
	LastError         *string
	At                time.Time
}

// Deferral is a breaker-open deferral: sending → retrying with no attempt consumed, or
// sending → failed (provider_unavailable) once the maximum deferral age has passed.
type Deferral struct {
	NotificationID string
	NextAttemptAt  time.Time
	Fail           bool
	LastError      *string
	At             time.Time
}

// BacklogCount is one row of the backlog gauge (FR-031).
type BacklogCount struct {
	Channel  domain.Channel
	Priority domain.Priority
	Status   domain.Status
	Count    int
}

// NotificationRepository persists notifications. Every state change is a guarded update; a
// rejected transition returns domain.ErrInvalidTransition or reports ok=false, never a silent
// no-op (Constitution III).
type NotificationRepository interface {
	// Create inserts n. On an idempotency-key conflict it returns the existing row with
	// created=false; the caller compares payload hashes.
	Create(ctx context.Context, n *domain.Notification) (stored *domain.Notification, created bool, err error)
	Get(ctx context.Context, id string) (*domain.Notification, error)
	ListAttempts(ctx context.Context, id string) ([]domain.Attempt, error)
	List(ctx context.Context, f ListFilter) ([]domain.Notification, error)

	// Cancel moves a cancellable notification to cancelled. When nothing changed (already
	// cancelled, or not cancellable) it returns the current row with Changed=false. An unknown id
	// is domain.ErrNotFound.
	Cancel(ctx context.Context, id string, at time.Time) (CancelOutcome, error)

	// RelayTx runs one channel's relay pass in a single transaction (research R4): advisory lock,
	// count queued, quota(queued) rows picked FOR UPDATE SKIP LOCKED, mark queued, publish, then
	// commit, or roll back if publish fails.
	RelayTx(ctx context.Context, ch domain.Channel, at time.Time, quota QuotaFunc, publish PublishFunc) (RelayResult, error)
	// StartSending is the guarded queued → sending claim; ok=false means drop the message.
	StartSending(ctx context.Context, id string, at time.Time) (n *domain.Notification, ok bool, err error)
	ApplyOutcome(ctx context.Context, o DeliveryOutcome) error
	DeferSending(ctx context.Context, d Deferral) error

	// ReapStaleSending recovers rows stuck in sending since before olderThan (research R5),
	// applying decide to each locked row. It returns the rows after recovery.
	ReapStaleSending(ctx context.Context, olderThan time.Time, limit int, decide ReapDecision) ([]domain.Notification, error)
	// RepublishStaleQueued republishes rows stuck in queued without changing their status.
	RepublishStaleQueued(ctx context.Context, olderThan, at time.Time, limit int, publish PublishFunc) (int, error)

	BacklogCounts(ctx context.Context) ([]BacklogCount, error)
}

// BatchCounts are status totals for one batch.
type BatchCounts map[domain.Status]int

// BatchCancelResult reports a batch cancel (FR-015).
type BatchCancelResult struct {
	Cancelled        int
	AlreadyCancelled int
	NotCancellable   int
	Rows             []CancelOutcome // the notifications this call cancelled
}

// CancelOutcome is the result of cancelling one notification.
type CancelOutcome struct {
	Notification *domain.Notification
	Previous     domain.Status // status before the call
	Changed      bool
}

// BatchItemKind is what happened to one batch item in the database.
type BatchItemKind int

const (
	// BatchItemCreated: a new notification in this batch.
	BatchItemCreated BatchItemKind = iota
	// BatchItemReplayed: the idempotency key already existed with the same payload; Stored is the
	// original notification.
	BatchItemReplayed
	// BatchItemConflict: the idempotency key already existed with a different payload; the item is
	// rejected and Stored is the existing notification.
	BatchItemConflict
)

// BatchItemOutcome pairs an item with its stored notification.
type BatchItemOutcome struct {
	Kind   BatchItemKind
	Stored *domain.Notification
}

// BatchRepository persists batches and their notifications.
type BatchRepository interface {
	// CreateBatch writes the batch row and its items in one transaction and returns one outcome
	// per item, in order. b.TotalRejected carries the items the caller already rejected;
	// conflicts are added to it, and created plus replayed items become TotalAccepted.
	CreateBatch(ctx context.Context, b domain.Batch, items []*domain.Notification) ([]BatchItemOutcome, error)
	GetBatch(ctx context.Context, id string) (*domain.Batch, BatchCounts, []domain.Notification, error)
	CancelBatch(ctx context.Context, id string, at time.Time) (BatchCancelResult, error)
}

// Percentiles are latency percentiles in milliseconds.
type Percentiles struct{ P50, P95, P99 float64 }

// ChannelStats is one channel's activity over a recent window (metrics summary, R16).
type ChannelStats struct {
	Channel   domain.Channel
	Sent      int          // notifications sent in the window
	Failed    int          // notifications that failed in the window
	Retried   int          // attempts in the window that led to a retry
	Attempts  int          // provider calls finished in the window
	Successes int          // of which successful
	E2E       *Percentiles // nil when nothing was sent
	Provider  *Percentiles // nil when no provider call finished
}

// SummaryRepository computes the metrics summary from the database (bonus, T116).
type SummaryRepository interface {
	BacklogCounts(ctx context.Context) ([]BacklogCount, error)
	ChannelStats(ctx context.Context, since time.Time) ([]ChannelStats, error)
}

// TemplateRepository persists templates (US8).
type TemplateRepository interface {
	CreateTemplate(ctx context.Context, t domain.Template) (*domain.Template, error)
	GetTemplate(ctx context.Context, id string) (*domain.Template, error)
	ListTemplates(ctx context.Context) ([]domain.Template, error)
}

// ---------------------------------------------------------------------------------------------
// Messaging and delivery
// ---------------------------------------------------------------------------------------------

// Message is the work item on the broker (contracts/events.md §1). The DB row is authoritative,
// so content isn't copied into it.
type Message struct {
	NotificationID string
	Channel        domain.Channel
	Priority       domain.Priority
	CorrelationID  string
	TraceParent    string // W3C trace context from the row; "" when tracing is off
	EnqueuedAt     time.Time
}

// Decision tells the consumer what to do with a delivered message.
type Decision int

const (
	// DecisionAck: the work is done or must be dropped (cancelled, duplicate, already terminal).
	DecisionAck Decision = iota
	// DecisionRequeue: release the message back to the queue; the row is still queued.
	DecisionRequeue
	// DecisionDeadLetter: the notification failed; route the message to the DLQ.
	DecisionDeadLetter
)

func (d Decision) String() string {
	switch d {
	case DecisionAck:
		return "ack"
	case DecisionRequeue:
		return "requeue"
	case DecisionDeadLetter:
		return "dead_letter"
	default:
		return fmt.Sprintf("decision(%d)", int(d))
	}
}

// MessageHandler processes one work message (the delivery pipeline).
type MessageHandler func(ctx context.Context, msg Message) Decision

// Publisher sends work messages. Publish returns nil only after every message is confirmed.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// ProviderRequest is what the external provider receives (contracts/provider.md).
type ProviderRequest struct {
	NotificationID string // sent as Idempotency-Key
	CorrelationID  string
	To             string
	Channel        domain.Channel
	Content        string
}

// ProviderResult is a classified provider response.
type ProviderResult struct {
	Outcome    domain.Outcome // success, transient or permanent
	HTTPStatus *int           // nil for network errors and timeouts
	MessageID  *string
	RetryAfter time.Duration // 0 when absent
	Error      string        // empty on success
	Latency    time.Duration
}

// Provider calls the external notification provider.
type Provider interface {
	Send(ctx context.Context, req ProviderRequest) ProviderResult
}

// ErrBreakerOpen means the channel's circuit is open (or half-open and busy).
var ErrBreakerOpen = errors.New("circuit breaker open")

// BreakerOpenError carries how long until the breaker next allows a trial call.
type BreakerOpenError struct {
	Channel domain.Channel
	RetryIn time.Duration
}

func (e *BreakerOpenError) Error() string {
	return fmt.Sprintf("%s for %s, retry in %s", ErrBreakerOpen, e.Channel, e.RetryIn)
}

// Unwrap lets errors.Is(err, ErrBreakerOpen) match.
func (e *BreakerOpenError) Unwrap() error { return ErrBreakerOpen }

// CircuitBreaker guards provider calls per channel (FR-024).
type CircuitBreaker interface {
	// Allow reports, without calling the provider, whether a call may proceed now. When it can't,
	// it returns a *BreakerOpenError.
	Allow(ch domain.Channel) error
	// Call runs fn through the channel's breaker. When the breaker rejects the call, fn isn't run
	// and the error is a *BreakerOpenError.
	Call(ctx context.Context, ch domain.Channel, fn func(context.Context) ProviderResult) (ProviderResult, error)
}

// RateLimiter grants delivery permits per channel across all workers (FR-016). Acquire blocks
// until the reserved slot arrives (or ctx ends) and returns that slot's local time; callers bound
// it with LIMITER_MAX_WAIT. Slots are spaced exactly 1/limit apart, so a caller that starts its
// provider call at slot + a constant offset keeps the calls themselves within the limit.
type RateLimiter interface {
	Acquire(ctx context.Context, ch domain.Channel) (slot time.Time, err error)
}

// ---------------------------------------------------------------------------------------------
// Events and metrics
// ---------------------------------------------------------------------------------------------

// StatusEvent is a committed status change (contracts/events.md §2).
type StatusEvent struct {
	NotificationID string
	BatchID        *string
	From           *domain.Status // nil on create
	To             domain.Status
	Attempts       int
	At             time.Time
	CorrelationID  string
}

// StatusBroadcaster publishes status events after commit. Delivery is best-effort: an error is
// logged and never fails the use case.
type StatusBroadcaster interface {
	Broadcast(ctx context.Context, ev StatusEvent) error
}

// Dead-letter reasons (notifications_dead_lettered_total{reason}).
const (
	ReasonExhausted           = "exhausted"
	ReasonPermanent           = "permanent"
	ReasonProviderUnavailable = "provider_unavailable"
)

// MetricsRecorder receives application-level measurements, so use cases don't depend on
// Prometheus (contracts/observability.md).
type MetricsRecorder interface {
	Delivered(ch domain.Channel, outcome domain.Outcome, providerLatency time.Duration)
	Retried(ch domain.Channel)
	DeadLettered(ch domain.Channel, reason string)
	EndToEnd(ch domain.Channel, p domain.Priority, latency time.Duration)
	LimiterWait(ch domain.Channel, wait time.Duration)
	LimiterTimeout(ch domain.Channel)
	RelayPublished(ch domain.Channel, n int)
	RelaySkipped(ch domain.Channel)
	Reaped(kind string, n int)
	Backlog(counts []BacklogCount)
	QueueDepth(queue string, n int)
}

// QueueInspector reports broker-side queue depths (a secondary signal next to the DB backlog).
type QueueInspector interface {
	Depths(ctx context.Context) (map[string]int, error)
}
