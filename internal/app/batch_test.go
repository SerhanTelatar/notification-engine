package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// memBatchRepo is an in-memory ports.BatchRepository with the same idempotency semantics as
// Postgres: a known key is a replay when the payload hash matches, a conflict otherwise.
type memBatchRepo struct {
	mu      sync.Mutex
	batches map[string]domain.Batch
	rows    map[string]*domain.Notification
	byKey   map[string]*domain.Notification
	calls   int
}

func newBatchRepo() *memBatchRepo {
	return &memBatchRepo{batches: map[string]domain.Batch{}, rows: map[string]*domain.Notification{}, byKey: map[string]*domain.Notification{}}
}

var _ ports.BatchRepository = (*memBatchRepo)(nil)

func (r *memBatchRepo) CreateBatch(_ context.Context, b domain.Batch, items []*domain.Notification) ([]ports.BatchItemOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	out := make([]ports.BatchItemOutcome, len(items))
	for i, n := range items {
		if n.IdempotencyKey != nil {
			if existing, ok := r.byKey[*n.IdempotencyKey]; ok {
				kind := ports.BatchItemReplayed
				if !bytes.Equal(existing.PayloadHash, n.PayloadHash) {
					kind = ports.BatchItemConflict
				}
				out[i] = ports.BatchItemOutcome{Kind: kind, Stored: clone(existing)}
				continue
			}
		}
		stored := clone(n)
		r.rows[n.ID] = stored
		if n.IdempotencyKey != nil {
			r.byKey[*n.IdempotencyKey] = stored
		}
		out[i] = ports.BatchItemOutcome{Kind: ports.BatchItemCreated, Stored: clone(stored)}
	}
	for _, o := range out {
		switch o.Kind {
		case ports.BatchItemConflict:
			b.TotalRejected++
		default:
			b.TotalAccepted++
		}
	}
	r.batches[b.ID] = b
	return out, nil
}

func (r *memBatchRepo) GetBatch(_ context.Context, id string) (*domain.Batch, ports.BatchCounts, []domain.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batches[id]
	if !ok {
		return nil, nil, nil, domain.ErrNotFound
	}
	counts := ports.BatchCounts{}
	var items []domain.Notification
	for _, n := range r.rows {
		if n.BatchID != nil && *n.BatchID == id {
			counts[n.Status]++
			items = append(items, *n)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return &b, counts, items, nil
}

func (r *memBatchRepo) CancelBatch(context.Context, string, time.Time) (ports.BatchCancelResult, error) {
	return ports.BatchCancelResult{}, errors.New("memBatchRepo.CancelBatch: not needed by these tests")
}

type batchFixture struct {
	repo   *memBatchRepo
	events *recBroadcaster
	uc     *Batcher
}

func newBatchFixture() batchFixture {
	f := batchFixture{repo: newBatchRepo(), events: &recBroadcaster{}}
	f.uc = NewBatcher(BatcherDeps{Repo: f.repo, IDs: &seqIDs{}, Clock: newClock(), Events: f.events, Log: quietLog, MaxAttempts: 5})
	return f
}

func smsItem(i int) CreateRequest {
	return CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: strp(fmt.Sprintf("message %d", i))}
}

func TestBatchPartialSuccessKeepsRequestOrder(t *testing.T) {
	f := newBatchFixture()
	var items []CreateRequest
	for i := range 10 {
		it := smsItem(i)
		switch i {
		case 2:
			it.Recipient = "not-e164"
		case 5:
			it.Content = strp(strings.Repeat("a", 161))
		case 9:
			it.Channel = "fax"
		}
		items = append(items, it)
	}
	res, err := f.uc.CreateBatch(context.Background(), BatchRequest{Items: items, CorrelationID: "c-batch"})
	if err != nil {
		t.Fatal(err)
	}
	if res.BatchID == "" || res.Accepted != 7 || res.Rejected != 3 || len(res.Items) != 10 {
		t.Fatalf("result %+v", res)
	}
	for i, it := range res.Items {
		wantRejected := i == 2 || i == 5 || i == 9
		if it.Index != i || it.Accepted == wantRejected {
			t.Fatalf("item %d: %+v", i, it)
		}
		if wantRejected {
			if len(it.Errors) == 0 || it.Notification != nil {
				t.Fatalf("rejected item %d needs field errors and no notification: %+v", i, it)
			}
			continue
		}
		n := it.Notification
		if n == nil || n.BatchID == nil || *n.BatchID != res.BatchID || n.CorrelationID != "c-batch" || n.Status != domain.StatusPending {
			t.Fatalf("accepted item %d: %+v", i, n)
		}
	}
	if res.Items[2].Errors[0].Field != "recipient" || res.Items[9].Errors[0].Field != "channel" {
		t.Fatalf("per-item field names: %+v / %+v", res.Items[2].Errors, res.Items[9].Errors)
	}
	if len(f.repo.rows) != 7 || len(f.events.events) != 7 {
		t.Fatalf("rows %d events %d, want 7 and 7", len(f.repo.rows), len(f.events.events))
	}
}

func TestBatchAllInvalidIsOneValidationError(t *testing.T) {
	f := newBatchFixture()
	items := []CreateRequest{{Channel: "sms", Recipient: "x", Content: strp("hi")}, {Channel: "email", Recipient: "nope", Content: nil}}
	_, err := f.uc.CreateBatch(context.Background(), BatchRequest{Items: items})
	var v *domain.ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	fields := map[string]bool{}
	for _, fe := range v.Fields {
		fields[fe.Field] = true
	}
	if !fields["items[0].recipient"] || !fields["items[1].content"] {
		t.Fatalf("fields %v, want items[i].field paths", v.Fields)
	}
	if f.repo.calls != 0 {
		t.Fatal("no batch may be created when every item is invalid (FR-008)")
	}
}

func TestBatchSizeLimits(t *testing.T) {
	for _, n := range []int{0, 1001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := newBatchFixture()
			items := make([]CreateRequest, n)
			for i := range items {
				items[i] = smsItem(i)
			}
			_, err := f.uc.CreateBatch(context.Background(), BatchRequest{Items: items})
			var v *domain.ValidationError
			if !errors.As(err, &v) || v.Fields[0].Field != "items" || f.repo.calls != 0 {
				t.Fatalf("%d items: err %v calls %d", n, err, f.repo.calls)
			}
		})
	}
	f := newBatchFixture()
	items := make([]CreateRequest, 1000)
	for i := range items {
		items[i] = smsItem(i)
	}
	if res, err := f.uc.CreateBatch(context.Background(), BatchRequest{Items: items}); err != nil || res.Accepted != 1000 {
		t.Fatalf("1,000 items is the maximum and must be accepted: %v %+v", err, res.Accepted)
	}
}

func TestBatchIdempotencyKeys(t *testing.T) {
	f := newBatchFixture()
	ctx := context.Background()

	// An earlier single notification owns key "prior".
	prior := smsItem(100)
	prior.IdempotencyKey = strp("prior")
	n, err := domain.NewNotification(domain.Draft{Recipient: prior.Recipient, Channel: prior.Channel, Content: *prior.Content,
		IdempotencyKey: prior.IdempotencyKey}, "prior-id", newClock().Now(), 5)
	if err != nil {
		t.Fatal(err)
	}
	f.repo.rows[n.ID], f.repo.byKey["prior"] = n, n

	first := smsItem(0)
	first.IdempotencyKey = strp("k")
	sameAgain := smsItem(0) // same payload, same key, later in the batch
	sameAgain.IdempotencyKey = strp("k")
	changed := smsItem(1) // same key, different payload
	changed.IdempotencyKey = strp("k")
	replayPrior := prior        // identical to the earlier request
	conflictPrior := smsItem(7) // key from earlier, different payload
	conflictPrior.IdempotencyKey = strp("prior")

	res, err := f.uc.CreateBatch(ctx, BatchRequest{Items: []CreateRequest{first, sameAgain, changed, replayPrior, conflictPrior}})
	if err != nil {
		t.Fatal(err)
	}
	it := res.Items
	if !it[0].Accepted || it[0].Replayed || !it[1].Accepted || it[1].Notification.ID != it[0].Notification.ID {
		t.Fatalf("in-batch duplicate must resolve to the first item: %+v / %+v", it[0], it[1])
	}
	if it[2].Accepted || it[2].Errors[0].Field != "idempotency_key" {
		t.Fatalf("same key, different payload in one batch must be rejected: %+v", it[2])
	}
	if !it[3].Accepted || !it[3].Replayed || it[3].Notification.ID != "prior-id" {
		t.Fatalf("a key from an earlier request with the same payload replays: %+v", it[3])
	}
	if it[4].Accepted || it[4].Errors[0].Field != "idempotency_key" {
		t.Fatalf("a key from an earlier request with a different payload conflicts: %+v", it[4])
	}
	if res.Accepted != 3 || res.Rejected != 2 {
		t.Fatalf("accepted %d rejected %d, want 3/2", res.Accepted, res.Rejected)
	}
	if got := len(f.events.events); got != 1 {
		t.Fatalf("events %d: only the newly created notification is announced", got)
	}
}

func TestGetBatch(t *testing.T) {
	f := newBatchFixture()
	items := []CreateRequest{smsItem(0), smsItem(1), {Channel: "sms", Recipient: "bad", Content: strp("x")}}
	res, err := f.uc.CreateBatch(context.Background(), BatchRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	// One of them has been sent meanwhile.
	f.repo.rows[res.Items[0].Notification.ID].Status = domain.StatusSent

	d, err := f.uc.GetBatch(context.Background(), res.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	sum := 0
	for _, c := range d.Counts {
		sum += c
	}
	if d.Batch.TotalAccepted != 2 || sum != 2 || d.Counts[domain.StatusSent] != 1 || d.Counts[domain.StatusPending] != 1 || len(d.Items) != 2 {
		t.Fatalf("detail %+v counts %v", d.Batch, d.Counts)
	}
	if _, err := f.uc.GetBatch(context.Background(), "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown batch: %v", err)
	}
}
