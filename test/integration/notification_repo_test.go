//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// newNotification builds a valid aggregate through the domain constructor.
func newNotification(t testing.TB, ch domain.Channel, mut func(*domain.Draft)) *domain.Notification {
	t.Helper()
	d := domain.Draft{Channel: string(ch), Content: "hello", CorrelationID: "c-test"}
	switch ch {
	case domain.ChannelSMS:
		d.Recipient = "+905551234567"
	case domain.ChannelEmail:
		d.Recipient = "ada@example.com"
	default:
		d.Recipient = "device-token-1"
	}
	if mut != nil {
		mut(&d)
	}
	id, _ := uuid.NewV7()
	n, err := domain.NewNotification(d, id.String(), time.Now(), 5)
	if err != nil {
		t.Fatalf("build notification: %v", err)
	}
	return n
}

func mustCreate(t testing.TB, repo *postgres.NotificationRepo, n *domain.Notification) *domain.Notification {
	t.Helper()
	stored, created, err := repo.Create(context.Background(), n)
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	return stored
}

func statusOf(t testing.TB, id string) (status string, attempts int) {
	t.Helper()
	if err := env.Pool.QueryRow(context.Background(),
		`SELECT status, attempts FROM notifications WHERE id = $1`, id).Scan(&status, &attempts); err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	return status, attempts
}

func TestRepoIdempotentCreate(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	ctx := context.Background()
	key := "order-42"

	first := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, func(d *domain.Draft) { d.IdempotencyKey = &key }))

	same := newNotification(t, domain.ChannelSMS, func(d *domain.Draft) { d.IdempotencyKey = &key })
	got, created, err := repo.Create(ctx, same)
	if err != nil || created || got.ID != first.ID || !bytes.Equal(got.PayloadHash, same.PayloadHash) {
		t.Fatalf("same payload: created=%v id=%s err=%v", created, got.ID, err)
	}

	different := newNotification(t, domain.ChannelSMS, func(d *domain.Draft) { d.IdempotencyKey = &key; d.Content = "changed" })
	got, created, err = repo.Create(ctx, different)
	if err != nil || created || got.ID != first.ID || bytes.Equal(got.PayloadHash, different.PayloadHash) {
		t.Fatalf("different payload must return the original for the caller to reject: created=%v err=%v", created, err)
	}
}

func TestRepoConcurrentSameKeyCreatesOneRow(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	key := "burst-key"
	var createdCount atomic.Int32
	ids := sync.Map{}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := newNotification(t, domain.ChannelSMS, func(d *domain.Draft) { d.IdempotencyKey = &key })
			got, created, err := repo.Create(context.Background(), n)
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			if created {
				createdCount.Add(1)
			}
			ids.Store(got.ID, true)
		}()
	}
	wg.Wait()

	distinct := 0
	ids.Range(func(any, any) bool { distinct++; return true })
	var rows int
	_ = env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&rows)
	if createdCount.Load() != 1 || distinct != 1 || rows != 1 {
		t.Fatalf("created=%d distinct ids=%d rows=%d, want 1/1/1", createdCount.Load(), distinct, rows)
	}
}

func TestRepoGuardedTransitions(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	ctx := context.Background()
	n := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil)) // pending

	// pending → sending is illegal: the guard matches 0 rows.
	if _, ok, err := repo.StartSending(ctx, n.ID, time.Now()); err != nil || ok {
		t.Fatalf("StartSending on pending: ok=%v err=%v", ok, err)
	}
	// Outcomes need a sending row.
	err := repo.ApplyOutcome(ctx, ports.DeliveryOutcome{NotificationID: n.ID, To: domain.StatusSent, At: time.Now(),
		Attempt: domain.Attempt{Outcome: domain.OutcomeSuccess, StartedAt: time.Now(), FinishedAt: time.Now()}})
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("ApplyOutcome on pending: %v, want ErrInvalidTransition", err)
	}
	if err := repo.DeferSending(ctx, ports.Deferral{NotificationID: n.ID, NextAttemptAt: time.Now(), At: time.Now()}); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("DeferSending on pending: %v", err)
	}
	if st, _ := statusOf(t, n.ID); st != "pending" {
		t.Fatalf("status changed to %s", st)
	}
}

func TestRepoCheckConstraints(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	n := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	for _, stmt := range []string{
		`UPDATE notifications SET status = 'delivered' WHERE id = $1`,
		`UPDATE notifications SET priority = 4 WHERE id = $1`,
		`UPDATE notifications SET attempts = max_attempts + 1 WHERE id = $1`,
		`UPDATE notifications SET status = 'sent' WHERE id = $1`, // sent requires sent_at
	} {
		if _, err := env.Pool.Exec(context.Background(), stmt, n.ID); err == nil || !strings.Contains(err.Error(), "check constraint") {
			t.Errorf("%s: want a check-constraint violation, got %v", stmt, err)
		}
	}
}

// moveTo forces a row into a status for test setup.
func moveTo(t testing.TB, id string, status domain.Status, updatedAgo time.Duration) {
	t.Helper()
	if _, err := env.Pool.Exec(context.Background(),
		`UPDATE notifications SET status = $2, updated_at = now() - $3::interval WHERE id = $1`,
		id, string(status), fmt.Sprintf("%d milliseconds", updatedAgo.Milliseconds())); err != nil {
		t.Fatalf("move %s to %s: %v", id, status, err)
	}
}

func TestRepoDeliveryLifecycle(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	ctx := context.Background()
	now := time.Now().UTC()

	sent := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, sent.ID, domain.StatusQueued, 0)
	claimed, ok, err := repo.StartSending(ctx, sent.ID, now)
	if err != nil || !ok || claimed.Status != domain.StatusSending {
		t.Fatalf("claim: %v %v %+v", ok, err, claimed)
	}
	if _, ok, _ := repo.StartSending(ctx, sent.ID, now); ok {
		t.Fatal("a second claim must fail (duplicate message)")
	}
	status, msgID := 202, "msg-1"
	if err := repo.ApplyOutcome(ctx, ports.DeliveryOutcome{NotificationID: sent.ID, To: domain.StatusSent, At: now,
		ProviderMessageID: &msgID,
		Attempt:           domain.Attempt{Outcome: domain.OutcomeSuccess, HTTPStatus: &status, StartedAt: now, FinishedAt: now, LatencyMS: 12}}); err != nil {
		t.Fatal(err)
	}
	var sentAt *time.Time
	var pmid *string
	var attemptNo int
	_ = env.Pool.QueryRow(ctx, `SELECT n.sent_at, n.provider_message_id, a.attempt_no FROM notifications n
		JOIN notification_attempts a ON a.notification_id = n.id WHERE n.id = $1`, sent.ID).Scan(&sentAt, &pmid, &attemptNo)
	if st, att := statusOf(t, sent.ID); st != "sent" || att != 1 || sentAt == nil || pmid == nil || *pmid != "msg-1" || attemptNo != 1 {
		t.Fatalf("sent row: %s attempts=%d sent_at=%v msg=%v attempt_no=%d", st, att, sentAt, pmid, attemptNo)
	}

	// Retrying with a long error: truncated to 1 KB in both tables.
	retry := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, retry.ID, domain.StatusSending, 0)
	long := strings.Repeat("e", 5000)
	if err := repo.ApplyOutcome(ctx, ports.DeliveryOutcome{NotificationID: retry.ID, To: domain.StatusRetrying, At: now,
		NextAttemptAt: now.Add(time.Minute), LastError: &long,
		Attempt:       domain.Attempt{Outcome: domain.OutcomeTransient, Error: &long, StartedAt: now, FinishedAt: now}}); err != nil {
		t.Fatal(err)
	}
	var lastLen, attLen int
	_ = env.Pool.QueryRow(ctx, `SELECT octet_length(n.last_error), octet_length(a.error) FROM notifications n
		JOIN notification_attempts a ON a.notification_id = n.id WHERE n.id = $1`, retry.ID).Scan(&lastLen, &attLen)
	if st, att := statusOf(t, retry.ID); st != "retrying" || att != 1 || lastLen != 1024 || attLen != 1024 {
		t.Fatalf("retrying row: %s attempts=%d last_error=%dB attempt.error=%dB", st, att, lastLen, attLen)
	}
}

func TestRepoRelayPublishesEachRowOnceAcrossConcurrentRelays(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	for range 300 {
		mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	}

	var mu sync.Mutex
	published := map[string]int{}
	var skippedLocked atomic.Int32
	publish := func(_ context.Context, rows []domain.Notification) error {
		time.Sleep(20 * time.Millisecond) // hold the transaction open
		mu.Lock()
		defer mu.Unlock()
		for _, r := range rows {
			published[r.ID]++
		}
		return nil
	}
	quota := func(int) int { return 25 }

	var wg sync.WaitGroup
	for range 8 { // 8 concurrent relays loop until the backlog is drained
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				res, err := repo.RelayTx(context.Background(), domain.ChannelSMS, time.Now(), quota, publish)
				if err != nil {
					t.Errorf("relay: %v", err)
					return
				}
				if res.Skipped == "locked" {
					skippedLocked.Add(1)
				}
				mu.Lock()
				done := len(published) == 300
				mu.Unlock()
				if done {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(published) != 300 {
		t.Fatalf("published %d distinct rows, want 300", len(published))
	}
	for id, n := range published {
		if n != 1 {
			t.Fatalf("row %s published %d times", id, n)
		}
	}
	if skippedLocked.Load() == 0 {
		t.Error("expected concurrent relays to observe the advisory lock")
	}
	var queued int
	_ = env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE status = 'queued'`).Scan(&queued)
	if queued != 300 {
		t.Fatalf("queued = %d, want 300", queued)
	}
}

func TestRepoRelayRollbackAndQuota(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	ctx := context.Background()
	a := mustCreate(t, repo, newNotification(t, domain.ChannelEmail, nil))

	// A publish failure rolls the transaction back: the row stays due.
	_, err := repo.RelayTx(ctx, domain.ChannelEmail, time.Now(), func(int) int { return 10 },
		func(context.Context, []domain.Notification) error { return errors.New("broker nack") })
	if err == nil {
		t.Fatal("publish failure must surface")
	}
	if st, _ := statusOf(t, a.ID); st != "pending" {
		t.Fatalf("rolled-back row is %s, want pending", st)
	}

	// The quota callback sees the queued count; <= 0 skips as backpressure.
	for range 3 {
		n := mustCreate(t, repo, newNotification(t, domain.ChannelEmail, nil))
		moveTo(t, n.ID, domain.StatusQueued, 0)
	}
	var seen int
	res, err := repo.RelayTx(ctx, domain.ChannelEmail, time.Now(), func(q int) int { seen = q; return 0 },
		func(context.Context, []domain.Notification) error { t.Fatal("must not publish"); return nil })
	if err != nil || res.Skipped != "backpressure" || seen != 3 {
		t.Fatalf("res=%+v err=%v queued seen=%d", res, err, seen)
	}

	// Scheduled rows aren't due before their time.
	future := time.Now().Add(time.Hour)
	mustCreate(t, repo, newNotification(t, domain.ChannelPush, func(d *domain.Draft) { d.ScheduledAt = &future }))
	res, err = repo.RelayTx(ctx, domain.ChannelPush, time.Now(), func(int) int { return 10 },
		func(context.Context, []domain.Notification) error { t.Fatal("not due yet"); return nil })
	if err != nil || res.Published != 0 {
		t.Fatalf("scheduled row relayed early: %+v %v", res, err)
	}
}

func TestRepoRelayPriorityOrder(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	for _, p := range []string{"low", "normal", "high", "low", "high"} {
		mustCreate(t, repo, newNotification(t, domain.ChannelSMS, func(d *domain.Draft) { d.Priority = p }))
	}
	var order []domain.Priority
	_, err := repo.RelayTx(context.Background(), domain.ChannelSMS, time.Now(), func(int) int { return 10 },
		func(_ context.Context, rows []domain.Notification) error {
			for _, r := range rows {
				order = append(order, r.Priority)
			}
			return nil
		})
	if err != nil || len(order) != 5 {
		t.Fatalf("order=%v err=%v", order, err)
	}
	for i := 1; i < len(order); i++ {
		if order[i] > order[i-1] {
			t.Fatalf("not in priority DESC order: %v", order)
		}
	}
}

func TestRepoCrashRecovery(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	ctx := context.Background()

	stale := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, stale.ID, domain.StatusSending, 10*time.Minute)
	fresh := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, fresh.ID, domain.StatusSending, 0)

	reaped, err := repo.ReapStaleSending(ctx, time.Now().Add(-2*time.Minute), 100, func(n domain.Notification) ports.DeliveryOutcome {
		msg := "abandoned"
		return ports.DeliveryOutcome{NotificationID: n.ID, To: domain.StatusRetrying, At: time.Now(), NextAttemptAt: time.Now(),
			LastError: &msg, Attempt: domain.Attempt{Outcome: domain.OutcomeAbandoned, StartedAt: n.UpdatedAt, FinishedAt: time.Now()}}
	})
	if err != nil || len(reaped) != 1 || reaped[0].ID != stale.ID {
		t.Fatalf("reaped=%v err=%v", reaped, err)
	}
	if st, att := statusOf(t, stale.ID); st != "retrying" || att != 1 {
		t.Fatalf("stale row: %s attempts=%d", st, att)
	}
	if st, _ := statusOf(t, fresh.ID); st != "sending" {
		t.Fatal("a fresh sending row must not be reaped")
	}

	lost := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, nil))
	moveTo(t, lost.ID, domain.StatusQueued, 20*time.Minute)
	var republished []string
	n, err := repo.RepublishStaleQueued(ctx, time.Now().Add(-15*time.Minute), time.Now(), 100,
		func(_ context.Context, rows []domain.Notification) error {
			for _, r := range rows {
				republished = append(republished, r.ID)
			}
			return nil
		})
	if err != nil || n != 1 || len(republished) != 1 || republished[0] != lost.ID {
		t.Fatalf("republished=%v n=%d err=%v", republished, n, err)
	}
	var age time.Duration
	_ = env.Pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM now() - updated_at)::bigint * 1000000000 FROM notifications WHERE id = $1`, lost.ID).Scan(&age)
	if st, _ := statusOf(t, lost.ID); st != "queued" || age > time.Minute {
		t.Fatalf("republished row must stay queued with a fresh updated_at: %s age=%v", st, age)
	}
}
