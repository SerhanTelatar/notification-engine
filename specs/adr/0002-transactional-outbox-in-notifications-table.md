# ADR 0002: Transactional outbox in the `notifications` table, with relay backpressure

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R4

## Context

The API must not lose an accepted notification (FR-025), and it must not dual-write to the DB and
the broker (Constitution III). If the API inserted a row and then published, a crash between the
two steps would leave a row with no message. Publishing first and then inserting would leave a
message with no row.

The planning input also had a race. The relay published, a consumer read the row before the relay
committed `queued`, saw `pending`, and dropped the message. The row was then committed as
`queued` with no message behind it.

A large burst (for example 100k SMS at 100/s, about 17 minutes) would also wait inside RabbitMQ
longer than the reaper's 15-minute `queued` threshold. The reaper would then republish thousands
of legitimately waiting messages.

## Decision

- **The `notifications` table is the outbox.** There's no separate outbox table. A row is due
  when `status IN ('pending','scheduled','retrying') AND next_attempt_at <= now()`. The API writes
  only to the DB.
- **The relay** runs in the worker on a 200 ms tick. For each channel, **in one DB transaction**,
  it:
  1. calls `pg_try_advisory_xact_lock(hashtext('relay:'||channel))`, and skips the channel if
     another relay holds the lock
  2. computes `quota = min(RELAY_BATCH_SIZE, RELAY_MAX_INFLIGHT_PER_CHANNEL − count(queued))`,
     and skips the channel if `quota ≤ 0` (**backpressure**)
  3. runs `SELECT … FOR UPDATE SKIP LOCKED ORDER BY priority DESC, next_attempt_at LIMIT quota`
  4. marks the picked rows `queued` with a guarded update
  5. publishes each message with publisher confirms and `mandatory=true`
  6. commits, or rolls back if any publish is nacked, returned or times out
- **The consumer claims in one transaction with two statements**: `SELECT … WHERE id=$1 FOR
  UPDATE`, then the guarded `UPDATE … SET status='sending' WHERE id=$1 AND status='queued'`. If
  the relay hasn't committed yet, the locking read waits on the relay's row lock and then returns
  the newest committed version. The consumer therefore sees either `queued` (proceed) or, after a
  rollback, `pending` (drop; the relay will publish again).
- **Why not just the guarded UPDATE**: under READ COMMITTED an UPDATE picks its candidates from its
  snapshot. While the relay transaction is open the row is still `pending` there, so the UPDATE
  matches 0 rows *without waiting* and the message is dropped. The integration tests caught this
  (T043); `TestRepoClaimWaitsForAnOpenRelayTransaction` now guards the behaviour.
- `RELAY_MAX_INFLIGHT_PER_CHANNEL` defaults to 1000. With the advisory lock, the cap holds exactly
  even with several worker replicas.

## Alternatives considered

- **Mark `queued`, commit, then publish**: a crash after commit leaves the row `queued` with no
  message, so the reaper would be needed on the happy path, not only for recovery.
- **A separate outbox table**: an extra write and extra cleanup, with no benefit when one row
  equals one message.
- **`LISTEN/NOTIFY` instead of polling**: more moving parts and connection handling; 200 ms polling
  already meets the 5 s scheduling bound (FR-018).
- **No backpressure**: unbounded broker depth, mass duplicate republishes from the reaper, and a
  larger redelivery storm when a consumer dies.

## Consequences

- No dual writes: anything the API acknowledged with 202 is durable and will be published.
- Row locks are held for one publish-and-confirm round trip per tick (a few milliseconds for
  500 messages).
- A crash between publish and commit causes a duplicate message, which the consumer guard drops
  (ADR 0003).
- Messages normally wait in `queued` for about `cap / rate` (10 s), far below the reaper
  threshold. If consumers stall for more than 15 minutes, at most `cap` messages per channel are
  republished.
- Priority still holds: the DB pick is `priority DESC`, and the RabbitMQ priority queue reorders
  whatever is already queued.
- The backlog lives mostly in Postgres. `notification_backlog` (DB) is therefore the main
  queue-depth signal, and the broker gauge is secondary.
