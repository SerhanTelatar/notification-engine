# Data Model: Event-Driven Notification Engine

**Feature**: [spec.md](./spec.md) | **Research**: [research.md](./research.md)

PostgreSQL is the single source of truth. Redis and RabbitMQ hold only derived or transient data
(see the end of this document).

## Domain value objects (`internal/domain`)

| Type | Values / rules |
|------|----------------|
| `Channel` | `sms`, `email`, `push`. Any other value is an `ErrInvalidChannel`. |
| `Priority` | `high`=3, `normal`=2 (default), `low`=1. The number equals the AMQP message priority. |
| `Status` | `pending`, `scheduled`, `queued`, `sending`, `sent`, `retrying`, `failed`, `cancelled` |
| `Recipient` | Validated per channel (below). |
| `Content` | Validated per channel after template rendering. |
| `Outcome` | `success`, `transient`, `permanent`, `deferred` (breaker open, not stored as an attempt), `abandoned` (reaped while `sending`) |

### Per-channel validation (FR-003)

| Channel | Recipient | Content limit | How it's counted |
|---------|-----------|---------------|------------------|
| sms | E.164: `^\+[1-9]\d{1,14}$` | ≤ 160 | Unicode code points (`utf8.RuneCountInString`) |
| email | `net/mail.ParseAddress` succeeds, and the result is a bare `addr-spec` (no display name) | ≤ 100,000 | code points |
| push | non-empty after trimming, ≤ 4,096 bytes | ≤ 4,000 | UTF-8 bytes (`len`) |

Content must not be empty after trimming. Content and `template_id` are mutually exclusive, and
exactly one of them is required (FR-004).

### State machine (FR-012)

```text
                 ┌──────────── cancelled ◄──────────────┐
                 │   ▲            ▲              ▲      │
  create ──► pending/scheduled ──► queued ──► sending ──┼─► sent      (terminal)
                                     ▲          │       │
                                     │          ├───────┴─► failed    (terminal)
                                     │          ▼
                                     └──── retrying ──► cancelled
```

| From | Allowed to | Triggered by |
|------|-----------|--------------|
| pending | queued, cancelled | relay; cancel |
| scheduled | queued, cancelled | relay (when `next_attempt_at <= now`); cancel |
| queued | sending, cancelled | consumer guard; cancel |
| sending | sent, retrying, failed | consumer outcome; reaper |
| retrying | queued, cancelled | relay; cancel |
| sent / failed / cancelled | — | terminal |

`domain.CanTransition(from, to) bool` is the single source of these rules. Repository methods take
the allowed `from` set from the domain and run
`UPDATE … WHERE id=$1 AND status = ANY($from)`. Zero rows affected means the transition was
rejected.

## Entities and tables

### `notifications`

| Column | Type | Null | Notes |
|--------|------|------|-------|
| `id` | `uuid` | no | PK, UUIDv7 |
| `batch_id` | `uuid` | yes | FK → `batches.id` |
| `idempotency_key` | `text` | yes | `UNIQUE`, at most 255 chars |
| `payload_hash` | `bytea` | no | SHA-256 of the canonical request (research R7) |
| `channel` | `text` | no | `CHECK (channel IN ('sms','email','push'))` |
| `recipient` | `text` | no | |
| `content` | `text` | no | rendered content |
| `priority` | `smallint` | no | `CHECK (priority BETWEEN 1 AND 3)`, default 2 |
| `status` | `text` | no | `CHECK` with the 8 statuses |
| `attempts` | `int` | no | default 0, number of provider calls made |
| `max_attempts` | `int` | no | stamped from config at create time (default 5) |
| `next_attempt_at` | `timestamptz` | no | when the relay may next pick the row |
| `scheduled_at` | `timestamptz` | yes | set only if it was in the future at create time |
| `template_id` | `uuid` | yes | FK → `templates.id` |
| `template_vars` | `jsonb` | yes | kept for audit |
| `provider_message_id` | `text` | yes | |
| `last_error` | `text` | yes | truncated to 1 KB; set to `provider_unavailable` when a breaker deferral exceeds `BREAKER_MAX_DEFER_AGE` (research R9) |
| `correlation_id` | `text` | no | |
| `created_at` / `updated_at` | `timestamptz` | no | `updated_at` is set on every transition |
| `sent_at` | `timestamptz` | yes | |
| `cancelled_at` / `failed_at` | `timestamptz` | yes | |

**Indexes**

- `notifications_due_idx ON (channel, priority DESC, next_attempt_at) WHERE status IN ('pending','scheduled','retrying')` is used by the relay (partial, so it stays small).
- `notifications_inflight_idx ON (channel, status, updated_at) WHERE status IN ('queued','sending')`
  is used by the relay's per-channel `queued` count (backpressure, research R4) and by the reaper.
  The partial index stays small because relay backpressure caps `queued` at
  `RELAY_MAX_INFLIGHT_PER_CHANNEL` per channel.
- `notifications_backlog_idx ON (channel, priority, status) WHERE status NOT IN ('sent','failed','cancelled')`
  is used by the backlog sampler and the summary.
- `notifications_list_idx ON (created_at DESC, id DESC)` is used for unfiltered or partially
  filtered keyset pagination.
- `notifications_list_filtered_idx ON (channel, status, created_at DESC, id DESC)` covers the
  combined filter.
- `notifications_batch_idx ON (batch_id) WHERE batch_id IS NOT NULL`.

**Invariants (as CHECK constraints)**

- `status = 'sent'` ⇔ `sent_at IS NOT NULL`
- `attempts <= max_attempts`
- `status = 'scheduled'` ⇒ `scheduled_at IS NOT NULL`

### `notification_attempts`

| Column | Type | Null | Notes |
|--------|------|------|-------|
| `id` | `bigint` identity | no | PK |
| `notification_id` | `uuid` | no | FK → `notifications.id` `ON DELETE CASCADE` |
| `attempt_no` | `int` | no | `UNIQUE (notification_id, attempt_no)`, which also blocks duplicate recording |
| `started_at` / `finished_at` | `timestamptz` | no | |
| `latency_ms` | `int` | no | |
| `http_status` | `int` | yes | null for network errors/timeouts |
| `outcome` | `text` | no | `success`, `transient`, `permanent`, `abandoned` |
| `error` | `text` | yes | truncated to 1 KB |

When the breaker is open, no attempt row is written, so the deferral doesn't count as an attempt
(FR-024).

### `batches`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK, UUIDv7 |
| `total_accepted` | `int` | |
| `total_rejected` | `int` | |
| `correlation_id` | `text` | |
| `created_at` | `timestamptz` | |

Status totals are derived on read: `SELECT status, count(*) … WHERE batch_id=$1 GROUP BY status`.

### `templates`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK |
| `name` | `text` | `UNIQUE`, 1–100 chars |
| `body` | `text` | ≤ 100,000 chars; only `{{.Var}}` field actions allowed |
| `variables` | `text[]` | derived from the body at create time, sorted |
| `created_at` | `timestamptz` | |

Templates are channel-agnostic; channel limits are applied to the rendered content (spec
assumption, research R12).

### Dead-letter entry

This isn't a separate table. A dead-letter entry is a `notifications` row with
`status='failed'` (with `last_error` and its last attempt), plus a copy of the message in the
RabbitMQ `notifications.dlq` queue for operators to inspect. Re-driving failed notifications is out
of scope.

### Status event (transient)

The JSON published to the Redis channel `notification-status` after each committed transition:

```json
{"notification_id":"…","batch_id":"…|null","from":"queued","to":"sending",
 "at":"2026-11-18T10:00:00.123Z","correlation_id":"…"}
```

It isn't persisted. See [contracts/events.md](./contracts/events.md).

## Relationships

```text
batches 1 ──< notifications >── 0..1 templates
notifications 1 ──< notification_attempts
```

## Write paths and their guards

| Use case | SQL guard |
|----------|-----------|
| Create | `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`, then compare `payload_hash` |
| Create batch | one `INSERT … SELECT FROM unnest(…) ON CONFLICT DO NOTHING RETURNING` together with the `batches` insert, in a single tx |
| Relay quota (per channel) | `pg_try_advisory_xact_lock(hashtext('relay:'\|\|$channel))`; `SELECT count(*) … WHERE channel=$1 AND status='queued'`; `quota = min(batch, max_inflight − count)` |
| Relay pick (per channel) | `SELECT … WHERE channel=$1 AND status IN (pending,scheduled,retrying) AND next_attempt_at <= now() ORDER BY priority DESC, next_attempt_at FOR UPDATE SKIP LOCKED LIMIT $quota` |
| Mark queued | `UPDATE … SET status='queued' WHERE id = ANY($1) AND status IN (pending,scheduled,retrying)` (same tx as the pick) |
| Start send | one tx: `SELECT status … WHERE id=$1 FOR UPDATE` (waits for an open relay tx), then `UPDATE … SET status='sending' WHERE id=$1 AND status='queued' RETURNING *` |
| Outcome | `UPDATE … WHERE id=$1 AND status='sending'` + `INSERT notification_attempts`, in one tx |
| Defer (breaker open) | one tx: `queued → sending`, then `sending → retrying` with `next_attempt_at = now + remaining cool-down`, `attempts` unchanged and no attempt row. If `now − max(created_at, scheduled_at) > BREAKER_MAX_DEFER_AGE`, the second step is instead `sending → failed` with `last_error='provider_unavailable'` and `failed_at = now()` |
| Cancel | `UPDATE … SET status='cancelled' WHERE id=$1 AND status IN (pending,scheduled,queued,retrying) RETURNING *`; on 0 rows, read the row to choose 404, 409 or 200 (already cancelled) |
| Cancel batch | the same predicate with `batch_id=$1`, `RETURNING id` |
| Reap sending | `UPDATE … WHERE status='sending' AND updated_at < now() - $timeout` (counts as an attempt, writes an `abandoned` attempt row) |
| Republish queued | `SELECT … WHERE status='queued' AND updated_at < now() - $timeout FOR UPDATE SKIP LOCKED`, publish, `UPDATE updated_at` |

## Non-durable stores

| Store | Key / name | Content | Effect of losing it |
|-------|------------|---------|---------------------|
| Redis | `ratelimit:{channel}` | GCRA theoretical arrival time | at most a brief burst of ≤ 1 extra call after a restart |
| Redis | pub/sub `notification-status` | status events | subscribers miss live events and recover via the snapshot or the status endpoint |
| RabbitMQ | `notifications.{channel}` | `{id, channel, priority, correlation_id, attempt}` | rows remain `queued`; the reaper republishes them |
| RabbitMQ | `notifications.dlq` | dead-lettered messages | the DB still holds `failed` rows |
