package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// MaxErrorBytes bounds stored error text (data-model.md notifications.last_error and
// notification_attempts.error).
const MaxErrorBytes = 1024

// Notification is the aggregate root: one message to one recipient on one channel. Fields mirror
// data-model.md § notifications. IDs are UUID strings, so the domain stays stdlib-only.
type Notification struct {
	ID                string
	BatchID           *string
	IdempotencyKey    *string
	PayloadHash       []byte
	Channel           Channel
	Recipient         string
	Content           string // rendered content: what the provider receives
	Priority          Priority
	Status            Status
	Attempts          int
	MaxAttempts       int
	NextAttemptAt     time.Time
	ScheduledAt       *time.Time
	TemplateID        *string
	TemplateVars      map[string]string
	ProviderMessageID *string
	LastError         *string
	CorrelationID     string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
	CancelledAt       *time.Time
	FailedAt          *time.Time
}

// Draft is a validated-at-construction request to create a notification. Content is the final
// content: given directly, or rendered from TemplateID + Variables by the application layer.
type Draft struct {
	Recipient      string
	Channel        string
	Content        string
	Priority       string
	ScheduledAt    *time.Time
	IdempotencyKey *string
	TemplateID     *string
	Variables      map[string]string
	CorrelationID  string
	BatchID        *string
}

// NewNotification validates d and builds a notification in its initial status. Every invalid field
// is reported at once. A schedule in the future gives `scheduled`; a past or absent schedule gives
// `pending` with delivery due now (spec Edge Cases).
func NewNotification(d Draft, id string, now time.Time, maxAttempts int) (*Notification, error) {
	now = now.UTC().Truncate(time.Microsecond) // Postgres timestamptz precision
	verr := &ValidationError{}

	ch, err := ParseChannel(d.Channel)
	if err != nil {
		verr.Add("channel", "must be one of sms, email, push")
	}
	prio, err := ParsePriority(d.Priority)
	if err != nil {
		verr.Add("priority", "must be one of high, normal, low")
	}
	if ch != "" {
		if msg := ValidateRecipient(ch, d.Recipient); msg != "" {
			verr.Add("recipient", msg)
		}
		if msg := ValidateContent(ch, d.Content); msg != "" {
			verr.Add("content", msg)
		}
	}
	if msg := ValidateIdempotencyKey(d.IdempotencyKey); msg != "" {
		verr.Add("idempotency_key", msg)
	}
	if err := verr.OrNil(); err != nil {
		return nil, err
	}
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max attempts must be >= 1, got %d", maxAttempts)
	}

	hash, err := PayloadHash(d)
	if err != nil {
		return nil, err
	}
	n := &Notification{
		ID:             id,
		BatchID:        d.BatchID,
		IdempotencyKey: d.IdempotencyKey,
		PayloadHash:    hash,
		Channel:        ch,
		Recipient:      d.Recipient,
		Content:        d.Content,
		Priority:       prio,
		Status:         StatusPending,
		MaxAttempts:    maxAttempts,
		NextAttemptAt:  now,
		TemplateID:     d.TemplateID,
		TemplateVars:   d.Variables,
		CorrelationID:  d.CorrelationID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if d.ScheduledAt != nil && d.ScheduledAt.After(now) {
		at := d.ScheduledAt.UTC().Truncate(time.Microsecond)
		n.Status = StatusScheduled
		n.ScheduledAt = &at
		n.NextAttemptAt = at
	}
	return n, nil
}

// DeliveryReference is max(created_at, scheduled_at). End-to-end latency and the breaker's
// maximum deferral age are measured from it.
func (n *Notification) DeliveryReference() time.Time {
	if n.ScheduledAt != nil && n.ScheduledAt.After(n.CreatedAt) {
		return *n.ScheduledAt
	}
	return n.CreatedAt
}

// canonicalPayload is what makes two submissions "the same" for idempotency (research R7).
type canonicalPayload struct {
	Channel     string            `json:"channel"`
	Recipient   string            `json:"recipient"`
	Content     *string           `json:"content,omitempty"`
	TemplateID  *string           `json:"template_id,omitempty"`
	Variables   map[string]string `json:"variables,omitempty"` // encoding/json sorts map keys
	Priority    string            `json:"priority"`
	ScheduledAt *string           `json:"scheduled_at,omitempty"`
}

// PayloadHash is the SHA-256 of a canonical JSON of the request (data-model.md
// notifications.payload_hash). For template requests it hashes template_id + variables rather
// than the rendered content, so a replay matches even if the template is later edited.
func PayloadHash(d Draft) ([]byte, error) {
	prio, err := ParsePriority(d.Priority)
	if err != nil {
		prio = PriorityNormal
	}
	c := canonicalPayload{Channel: d.Channel, Recipient: d.Recipient, Priority: prio.String()}
	if d.TemplateID != nil && *d.TemplateID != "" {
		c.TemplateID, c.Variables = d.TemplateID, d.Variables
	} else {
		content := d.Content
		c.Content = &content
	}
	if d.ScheduledAt != nil {
		s := d.ScheduledAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
		c.ScheduledAt = &s
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("canonical payload: %w", err)
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

// TruncateError limits error text to MaxErrorBytes without splitting a UTF-8 sequence.
func TruncateError(s string) string {
	if len(s) <= MaxErrorBytes {
		return s
	}
	cut := MaxErrorBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// Outcome classifies a delivery attempt (data-model.md notification_attempts.outcome).
type Outcome string

const (
	OutcomeSuccess   Outcome = "success"
	OutcomeTransient Outcome = "transient"
	OutcomePermanent Outcome = "permanent"
	OutcomeAbandoned Outcome = "abandoned" // reaped while sending (crash recovery)
	// OutcomeDeferred is a breaker-open deferral. It is never stored as an attempt (FR-024) and
	// only appears in metrics.
	OutcomeDeferred Outcome = "deferred"
)

// Attempt is one provider call (data-model.md § notification_attempts).
type Attempt struct {
	NotificationID string
	AttemptNo      int
	StartedAt      time.Time
	FinishedAt     time.Time
	LatencyMS      int
	HTTPStatus     *int
	Outcome        Outcome
	Error          *string
}
