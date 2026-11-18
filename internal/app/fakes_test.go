package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// In-memory port fakes for use-case unit tests (Constitution V: fakes of ports only). They're
// small and honest: the repository applies the same guarded-transition rules as Postgres, via
// domain.CanTransition, so a use case that asks for an illegal transition fails here too.

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// trace records cross-fake call order, e.g. "acquire" before "start".
type trace struct {
	mu    sync.Mutex
	steps []string
}

func (t *trace) add(s string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.steps = append(t.steps, s)
}

func (t *trace) list() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.steps...)
}

// --- Clock and ids --------------------------------------------------------------------------

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type seqIDs struct {
	mu sync.Mutex
	n  int
}

func (s *seqIDs) NewID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("00000000-0000-7000-8000-%012d", s.n)
}

// --- Repository -----------------------------------------------------------------------------

type memRepo struct {
	mu       sync.Mutex
	rows     map[string]*domain.Notification
	byKey    map[string]string
	attempts map[string][]domain.Attempt
	locked   map[domain.Channel]bool // simulate another relay holding the advisory lock
	trace    *trace

	createErr error
	startErr  error
	applyErr  error
}

func newRepo() *memRepo {
	return &memRepo{
		rows:     map[string]*domain.Notification{},
		byKey:    map[string]string{},
		attempts: map[string][]domain.Attempt{},
		locked:   map[domain.Channel]bool{},
	}
}

var _ ports.NotificationRepository = (*memRepo)(nil)

func clone(n *domain.Notification) *domain.Notification {
	c := *n
	return &c
}

// put inserts a row directly (test setup).
func (r *memRepo) put(n *domain.Notification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[n.ID] = clone(n)
	if n.IdempotencyKey != nil {
		r.byKey[*n.IdempotencyKey] = n.ID
	}
}

// get returns a copy of a row (test assertions).
func (r *memRepo) get(id string) *domain.Notification {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.rows[id]; ok {
		return clone(n)
	}
	return nil
}

func (r *memRepo) attemptsOf(id string) []domain.Attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.Attempt(nil), r.attempts[id]...)
}

// transition applies a guarded update: it fails exactly when Postgres would match 0 rows.
func (r *memRepo) transition(id string, to domain.Status) (*domain.Notification, bool) {
	n, ok := r.rows[id]
	if !ok || !domain.CanTransition(n.Status, to) {
		return nil, false
	}
	n.Status = to
	return n, true
}

func (r *memRepo) Create(_ context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.createErr != nil {
		return nil, false, r.createErr
	}
	if n.IdempotencyKey != nil {
		if id, ok := r.byKey[*n.IdempotencyKey]; ok {
			return clone(r.rows[id]), false, nil
		}
		r.byKey[*n.IdempotencyKey] = n.ID
	}
	r.rows[n.ID] = clone(n)
	return clone(n), true, nil
}

func (r *memRepo) Get(_ context.Context, id string) (*domain.Notification, error) {
	if n := r.get(id); n != nil {
		return n, nil
	}
	return nil, domain.ErrNotFound
}

func (r *memRepo) ListAttempts(_ context.Context, id string) ([]domain.Attempt, error) {
	return r.attemptsOf(id), nil
}

func (r *memRepo) List(context.Context, ports.ListFilter) ([]domain.Notification, error) {
	return nil, errors.New("memRepo.List: not needed by these tests")
}

func (r *memRepo) Cancel(_ context.Context, id string, at time.Time) (ports.CancelOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.rows[id]
	if !ok {
		return ports.CancelOutcome{}, domain.ErrNotFound
	}
	prev := n.Status
	if !prev.In(domain.CancellableFrom) {
		return ports.CancelOutcome{Notification: clone(n), Previous: prev}, nil
	}
	cancelledAt := at
	n.Status, n.CancelledAt, n.UpdatedAt = domain.StatusCancelled, &cancelledAt, at
	return ports.CancelOutcome{Notification: clone(n), Previous: prev, Changed: true}, nil
}

func (r *memRepo) RelayTx(ctx context.Context, ch domain.Channel, at time.Time, quota ports.QuotaFunc, publish ports.PublishFunc) (ports.RelayResult, error) {
	r.mu.Lock()
	if r.locked[ch] {
		r.mu.Unlock()
		return ports.RelayResult{Skipped: "locked"}, nil
	}
	queued := 0
	var due []*domain.Notification
	for _, n := range r.rows {
		if n.Channel != ch {
			continue
		}
		if n.Status == domain.StatusQueued {
			queued++
		}
		if n.Status.In(domain.DueFrom) && !n.NextAttemptAt.After(at) {
			due = append(due, n)
		}
	}
	limit := quota(queued)
	if limit <= 0 {
		r.mu.Unlock()
		return ports.RelayResult{Skipped: "backpressure"}, nil
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].Priority != due[j].Priority {
			return due[i].Priority > due[j].Priority
		}
		return due[i].NextAttemptAt.Before(due[j].NextAttemptAt)
	})
	if len(due) > limit {
		due = due[:limit]
	}
	// Snapshot for rollback, then mark queued.
	before := map[string]domain.Notification{}
	picked := make([]domain.Notification, 0, len(due))
	for _, n := range due {
		before[n.ID] = *n
		picked = append(picked, *n) // pre-relay status, like the Postgres repository
		n.Status, n.UpdatedAt = domain.StatusQueued, at
	}
	r.mu.Unlock()

	if len(picked) == 0 {
		return ports.RelayResult{}, nil
	}
	if err := publish(ctx, picked); err != nil {
		r.mu.Lock()
		for id, n := range before {
			restored := n
			r.rows[id] = &restored
		}
		r.mu.Unlock()
		return ports.RelayResult{}, err
	}
	return ports.RelayResult{Published: len(picked)}, nil
}

func (r *memRepo) StartSending(_ context.Context, id string, at time.Time) (*domain.Notification, bool, error) {
	r.trace.add("start")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startErr != nil {
		return nil, false, r.startErr
	}
	n, ok := r.transition(id, domain.StatusSending)
	if !ok {
		return nil, false, nil
	}
	n.UpdatedAt = at
	return clone(n), true, nil
}

func (r *memRepo) ApplyOutcome(_ context.Context, o ports.DeliveryOutcome) error {
	r.trace.add("apply:" + string(o.To))
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applyErr != nil {
		return r.applyErr
	}
	n, ok := r.rows[o.NotificationID]
	if !ok || n.Status != domain.StatusSending || !domain.CanTransition(domain.StatusSending, o.To) {
		return fmt.Errorf("apply outcome %s → %s: %w", n.Status, o.To, domain.ErrInvalidTransition)
	}
	r.applyLocked(n, o)
	return nil
}

func (r *memRepo) applyLocked(n *domain.Notification, o ports.DeliveryOutcome) {
	n.Status, n.UpdatedAt, n.LastError = o.To, o.At, o.LastError
	n.Attempts++
	a := o.Attempt
	a.NotificationID, a.AttemptNo = n.ID, n.Attempts
	r.attempts[n.ID] = append(r.attempts[n.ID], a)
	switch o.To {
	case domain.StatusSent:
		at := o.At
		n.SentAt, n.ProviderMessageID = &at, o.ProviderMessageID
	case domain.StatusRetrying:
		n.NextAttemptAt = o.NextAttemptAt
	case domain.StatusFailed:
		at := o.At
		n.FailedAt = &at
	}
}

func (r *memRepo) DeferSending(_ context.Context, d ports.Deferral) error {
	r.trace.add("defer")
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.rows[d.NotificationID]
	if !ok || n.Status != domain.StatusSending {
		return domain.ErrInvalidTransition
	}
	n.UpdatedAt = d.At
	if d.Fail {
		at := d.At
		n.Status, n.LastError, n.FailedAt = domain.StatusFailed, d.LastError, &at
		return nil
	}
	n.Status, n.NextAttemptAt = domain.StatusRetrying, d.NextAttemptAt
	return nil
}

func (r *memRepo) ReapStaleSending(_ context.Context, olderThan time.Time, limit int, decide ports.ReapDecision) ([]domain.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Notification
	for _, n := range r.rows {
		if len(out) >= limit {
			break
		}
		if n.Status == domain.StatusSending && n.UpdatedAt.Before(olderThan) {
			o := decide(*n)
			if !domain.CanTransition(n.Status, o.To) {
				return nil, domain.ErrInvalidTransition
			}
			r.applyLocked(n, o)
			out = append(out, *n)
		}
	}
	return out, nil
}

func (r *memRepo) RepublishStaleQueued(ctx context.Context, olderThan, at time.Time, limit int, publish ports.PublishFunc) (int, error) {
	r.mu.Lock()
	var stale []domain.Notification
	for _, n := range r.rows {
		if len(stale) < limit && n.Status == domain.StatusQueued && n.UpdatedAt.Before(olderThan) {
			stale = append(stale, *n)
		}
	}
	r.mu.Unlock()
	if len(stale) == 0 {
		return 0, nil
	}
	if err := publish(ctx, stale); err != nil {
		return 0, err
	}
	r.mu.Lock()
	for _, n := range stale {
		r.rows[n.ID].UpdatedAt = at
	}
	r.mu.Unlock()
	return len(stale), nil
}

func (r *memRepo) BacklogCounts(context.Context) ([]ports.BacklogCount, error) {
	return nil, errors.New("memRepo.BacklogCounts: not needed by these tests")
}

// --- Publisher ------------------------------------------------------------------------------

type recPublisher struct {
	mu   sync.Mutex
	msgs []ports.Message
	err  error
}

func (p *recPublisher) Publish(_ context.Context, msgs []ports.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func (p *recPublisher) published() []ports.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.Message(nil), p.msgs...)
}

// --- Provider -------------------------------------------------------------------------------

type scriptProvider struct {
	mu      sync.Mutex
	results []ports.ProviderResult // consumed in order; then success
	calls   []ports.ProviderRequest
	trace   *trace
}

func (p *scriptProvider) Send(_ context.Context, req ports.ProviderRequest) ports.ProviderResult {
	p.trace.add("send")
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, req)
	if len(p.results) > 0 {
		r := p.results[0]
		p.results = p.results[1:]
		return r
	}
	id := fmt.Sprintf("msg-%d", len(p.calls))
	status := 202
	return ports.ProviderResult{Outcome: domain.OutcomeSuccess, HTTPStatus: &status, MessageID: &id, Latency: 5 * time.Millisecond}
}

func (p *scriptProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func httpResult(status int, outcome domain.Outcome, msg string) ports.ProviderResult {
	s := status
	return ports.ProviderResult{Outcome: outcome, HTTPStatus: &s, Error: msg, Latency: time.Millisecond}
}

// --- Limiter --------------------------------------------------------------------------------

type fakeLimiter struct {
	mu    sync.Mutex
	calls int
	err   error // returned by every Acquire when set
	block bool  // wait for ctx instead of granting
	trace *trace
}

func (l *fakeLimiter) Acquire(ctx context.Context, _ domain.Channel) (time.Time, error) {
	l.trace.add("acquire")
	l.mu.Lock()
	l.calls++
	err, block := l.err, l.block
	l.mu.Unlock()
	if block {
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	}
	return time.Now(), err // the slot is "now": real time, because pacing uses real sleeps
}

func (l *fakeLimiter) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// --- Breaker --------------------------------------------------------------------------------

type fakeBreaker struct {
	mu       sync.Mutex
	open     bool
	retryIn  time.Duration
	rejectOn bool // reject at Call time even though Allow passed (opened meanwhile)
}

func (b *fakeBreaker) openErr(ch domain.Channel) error {
	return &ports.BreakerOpenError{Channel: ch, RetryIn: b.retryIn}
}

func (b *fakeBreaker) Allow(ch domain.Channel) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open {
		return b.openErr(ch)
	}
	return nil
}

func (b *fakeBreaker) Call(ctx context.Context, ch domain.Channel, fn func(context.Context) ports.ProviderResult) (ports.ProviderResult, error) {
	b.mu.Lock()
	reject := b.open || b.rejectOn
	b.mu.Unlock()
	if reject {
		return ports.ProviderResult{}, b.openErr(ch)
	}
	return fn(ctx), nil
}

// --- Broadcaster ----------------------------------------------------------------------------

type recBroadcaster struct {
	mu     sync.Mutex
	events []ports.StatusEvent
	err    error
}

func (b *recBroadcaster) Broadcast(_ context.Context, ev ports.StatusEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
	return b.err
}

func (b *recBroadcaster) statuses(id string) []domain.Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []domain.Status
	for _, e := range b.events {
		if e.NotificationID == id {
			out = append(out, e.To)
		}
	}
	return out
}

// --- Metrics --------------------------------------------------------------------------------

type recMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func newMetrics() *recMetrics { return &recMetrics{counts: map[string]int{}} }

func (m *recMetrics) inc(k string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[k] += n
}

func (m *recMetrics) get(k string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[k]
}

func (m *recMetrics) Delivered(ch domain.Channel, o domain.Outcome, _ time.Duration) {
	m.inc("delivered:"+string(ch)+":"+string(o), 1)
}
func (m *recMetrics) Retried(ch domain.Channel) { m.inc("retried:"+string(ch), 1) }
func (m *recMetrics) DeadLettered(ch domain.Channel, reason string) {
	m.inc("dead:"+string(ch)+":"+reason, 1)
}
func (m *recMetrics) EndToEnd(ch domain.Channel, _ domain.Priority, _ time.Duration) {
	m.inc("e2e:"+string(ch), 1)
}
func (m *recMetrics) LimiterWait(domain.Channel, time.Duration) {}
func (m *recMetrics) LimiterTimeout(ch domain.Channel)          { m.inc("limiter_timeout:"+string(ch), 1) }
func (m *recMetrics) RelayPublished(ch domain.Channel, n int) {
	m.inc("relay_published:"+string(ch), n)
}
func (m *recMetrics) RelaySkipped(ch domain.Channel) { m.inc("relay_skipped:"+string(ch), 1) }
func (m *recMetrics) Reaped(kind string, n int)      { m.inc("reaped:"+kind, n) }
func (m *recMetrics) Backlog([]ports.BacklogCount)   {}

// --- Logs -----------------------------------------------------------------------------------

// logCapture is a JSON logger whose output tests can search.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *logCapture) logger() *slog.Logger { return slog.New(slog.NewJSONHandler(c, nil)) }

// --- Fixtures -------------------------------------------------------------------------------

func strp(s string) *string { return &s }

// queuedNotification returns a row as the relay leaves it: queued, ready for a consumer.
func queuedNotification(id string, ch domain.Channel, now time.Time) *domain.Notification {
	return &domain.Notification{
		ID: id, Channel: ch, Recipient: "+905551234567", Content: "hi", Priority: domain.PriorityNormal,
		Status: domain.StatusQueued, MaxAttempts: 5, NextAttemptAt: now, CorrelationID: "c-" + id,
		CreatedAt: now, UpdatedAt: now, PayloadHash: make([]byte, 32),
	}
}
