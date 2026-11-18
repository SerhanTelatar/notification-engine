# ADR 0003: At-least-once delivery, made effectively-once with idempotency

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R7

## Context

Exactly-once delivery to an external HTTP provider isn't achievable: the provider can accept a
message and the response can be lost. The system still has to guarantee:

- the same idempotency key never creates a second notification or a second send (FR-006, SC-005)
- redelivered or duplicate queue messages never cause a second provider call for a notification
  that has already moved past `queued` (FR-026)

## Decision

- **Delivery semantics are at-least-once.** Exactly-once isn't claimed anywhere.
- **Submission idempotency**:
  - `notifications.idempotency_key` is `UNIQUE` (nullable).
  - `payload_hash` is the SHA-256 of a canonical JSON of
    `{channel, recipient, content | template_id + sorted variables, priority, scheduled_at}`.
  - Inserts use `ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`. When no row is returned,
    the existing row is read: the same hash returns the original (202 +
    `Idempotent-Replayed: true`); a different hash returns 409 `idempotency_conflict`.
  - The key can come from the `Idempotency-Key` header or the body field. If both are given and
    differ, the request is a 400.
  - Inside a batch, later items with a repeated key resolve to the first item's notification.
- **Processing idempotency**: the consumer only calls the provider after the guarded
  `UPDATE … SET status='sending' WHERE id=$1 AND status='queued'` affects one row. Messages for
  rows in any other state are acked and dropped.
- **Provider-side help**: the notification id is sent as an `Idempotency-Key` header, so a
  provider that deduplicates can also close the window below.

## The duplicate window that remains (documented, not hidden)

Suppose the provider accepts the call but the worker dies before committing `sent`. The row stays
`sending`. After `REAPER_SENDING_TIMEOUT` (2 min) the reaper moves it to `retrying` and records
an `abandoned` attempt, and the notification **may be delivered twice**. This is the only path to
a duplicate. Duplicate submissions and duplicate messages can't cause one.

## Alternatives considered

- **An idempotency cache in Redis**: not durable, and Redis must never be the source of truth
  (Constitution III).
- **Idempotency at the HTTP layer only** (caching responses): doesn't cover batch items, and does
  nothing for duplicate messages.
- **A distributed transaction or two-phase commit with the provider**: the provider doesn't
  support it, and it would be far too complex for the gain.

## Consequences

- Clients can safely retry `POST /notifications` and batch submissions with the same key.
- A concurrent submit with the same key waits on the unique index and then resolves to the
  original row: one row, one provider call.
- Status checks make redelivery after a consumer crash, the relay's crash window (ADR 0002) and
  the reaper's republishes all harmless.
- The README and the API docs state the remaining crash window explicitly.
