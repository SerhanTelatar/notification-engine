-- Notification queries (sqlc). Every state change is a guarded update: the WHERE clause names the
-- allowed source status, so an illegal transition matches 0 rows (Constitution III,
-- data-model.md "Write paths and their guards").

-- name: InsertNotification :one
-- ON CONFLICT DO NOTHING returns no row for an existing idempotency key (pgx.ErrNoRows); the
-- repository then reads the original and compares payload hashes (research R7).
INSERT INTO notifications (
    id, batch_id, idempotency_key, payload_hash, channel, recipient, content, priority, status,
    attempts, max_attempts, next_attempt_at, scheduled_at, template_id, template_vars,
    correlation_id, created_at, updated_at
) VALUES (
    @id, @batch_id, @idempotency_key, @payload_hash, @channel, @recipient, @content, @priority, @status,
    0, @max_attempts, @next_attempt_at, @scheduled_at, @template_id, @template_vars,
    @correlation_id, @created_at, @created_at
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;

-- name: GetByIdempotencyKey :one
SELECT * FROM notifications WHERE idempotency_key = @idempotency_key;

-- ---------------------------------------------------------------------------------------------
-- Outbox relay (research R4). All of these run inside one transaction per channel.
-- ---------------------------------------------------------------------------------------------

-- name: RelayTryLock :one
-- Serialises relays per channel across worker replicas, so the backpressure cap is exact.
-- Released automatically at commit or rollback.
SELECT pg_try_advisory_xact_lock(hashtext('relay:' || @channel::text)) AS locked;

-- name: CountQueued :one
SELECT count(*)::int AS queued FROM notifications WHERE channel = @channel AND status = 'queued';

-- name: PickDue :many
SELECT * FROM notifications
WHERE channel = @channel
  AND status IN ('pending', 'scheduled', 'retrying')
  AND next_attempt_at <= @now
ORDER BY priority DESC, next_attempt_at
LIMIT @pick_limit
FOR UPDATE SKIP LOCKED;

-- name: MarkQueued :many
UPDATE notifications SET status = 'queued', updated_at = @now
WHERE id = ANY(@ids::uuid[]) AND status IN ('pending', 'scheduled', 'retrying')
RETURNING id;

-- ---------------------------------------------------------------------------------------------
-- Delivery (research R6)
-- ---------------------------------------------------------------------------------------------

-- name: LockForClaim :one
-- First step of the claim. A locking read with no status predicate waits for a relay transaction
-- that still holds the row, then returns the newest committed version. A plain guarded UPDATE
-- would not: under READ COMMITTED it picks candidates from its snapshot, where the row is still
-- pending, so it would match 0 rows without waiting and the message would be dropped.
SELECT status FROM notifications WHERE id = @id FOR UPDATE;

-- name: StartSending :one
-- Second step, same transaction: the atomic cancel check (FR-014). Only a queued row can be
-- claimed; 0 rows means drop the message (cancelled, duplicate or already done).
UPDATE notifications SET status = 'sending', updated_at = @now
WHERE id = @id AND status = 'queued'
RETURNING *;

-- name: MarkSent :one
UPDATE notifications
SET status = 'sent', attempts = attempts + 1, provider_message_id = @provider_message_id,
    last_error = NULL, sent_at = @now::timestamptz, updated_at = @now::timestamptz
WHERE id = @id AND status = 'sending'
RETURNING attempts;

-- name: MarkRetrying :one
UPDATE notifications
SET status = 'retrying', attempts = attempts + 1, next_attempt_at = @next_attempt_at,
    last_error = @last_error, updated_at = @now
WHERE id = @id AND status = 'sending'
RETURNING attempts;

-- name: MarkFailed :one
UPDATE notifications
SET status = 'failed', attempts = attempts + 1, last_error = @last_error,
    failed_at = @now::timestamptz, updated_at = @now::timestamptz
WHERE id = @id AND status = 'sending'
RETURNING attempts;

-- name: InsertAttempt :exec
INSERT INTO notification_attempts (
    notification_id, attempt_no, started_at, finished_at, latency_ms, http_status, outcome, error
) VALUES (
    @notification_id, @attempt_no, @started_at, @finished_at, @latency_ms, @http_status, @outcome, @error
);

-- name: DeferSending :one
-- Breaker open (FR-024): back to retrying without consuming an attempt.
UPDATE notifications SET status = 'retrying', next_attempt_at = @next_attempt_at, updated_at = @now
WHERE id = @id AND status = 'sending'
RETURNING id;

-- name: DeferFail :one
-- Breaker deferral older than BREAKER_MAX_DEFER_AGE: give up as provider_unavailable.
UPDATE notifications SET status = 'failed', last_error = @last_error, failed_at = @now::timestamptz, updated_at = @now::timestamptz
WHERE id = @id AND status = 'sending'
RETURNING id;

-- ---------------------------------------------------------------------------------------------
-- Crash recovery (research R5)
-- ---------------------------------------------------------------------------------------------

-- name: ReapStaleSending :many
SELECT * FROM notifications
WHERE status = 'sending' AND updated_at < @older_than
ORDER BY updated_at
LIMIT @pick_limit
FOR UPDATE SKIP LOCKED;

-- name: PickStaleQueued :many
SELECT * FROM notifications
WHERE status = 'queued' AND updated_at < @older_than
ORDER BY updated_at
LIMIT @pick_limit
FOR UPDATE SKIP LOCKED;

-- name: TouchQueued :exec
UPDATE notifications SET updated_at = @now
WHERE id = ANY(@ids::uuid[]) AND status = 'queued';

-- ---------------------------------------------------------------------------------------------
-- Status lookup (US2, FR-009)
-- ---------------------------------------------------------------------------------------------

-- name: GetNotification :one
SELECT * FROM notifications WHERE id = @id;

-- name: ListAttempts :many
SELECT * FROM notification_attempts WHERE notification_id = @notification_id ORDER BY attempt_no;

-- ---------------------------------------------------------------------------------------------
-- Cancellation (US5, FR-013/014/015). Same lock-then-guard pattern as the claim: LockForClaim
-- waits for an open relay transaction, so the guard sees the committed status.
-- ---------------------------------------------------------------------------------------------

-- name: CancelNotification :one
UPDATE notifications
SET status = 'cancelled', cancelled_at = @now::timestamptz, updated_at = @now::timestamptz
WHERE id = @id AND status IN ('pending', 'scheduled', 'queued', 'retrying')
RETURNING *;

-- name: LockBatchItems :many
SELECT id, status FROM notifications WHERE batch_id = @batch_id ORDER BY id FOR UPDATE;

-- name: CancelMany :many
UPDATE notifications
SET status = 'cancelled', cancelled_at = @now::timestamptz, updated_at = @now::timestamptz
WHERE id = ANY(@ids::uuid[]) AND status IN ('pending', 'scheduled', 'queued', 'retrying')
RETURNING *;

-- ---------------------------------------------------------------------------------------------
-- Listing (US6, FR-011): keyset pagination, newest first (research R13)
-- ---------------------------------------------------------------------------------------------

-- name: ListNotifications :many
-- Optional filters are NULL when absent. The (created_at, id) row comparison continues strictly
-- after the cursor, so rows inserted while a client pages can't shift later pages.
SELECT * FROM notifications
WHERE (sqlc.narg(status)::text IS NULL OR status = sqlc.narg(status)::text)
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (sqlc.narg(from_ts)::timestamptz IS NULL OR created_at >= sqlc.narg(from_ts)::timestamptz)
  AND (sqlc.narg(to_ts)::timestamptz IS NULL OR created_at < sqlc.narg(to_ts)::timestamptz)
  AND (sqlc.narg(after_created_at)::timestamptz IS NULL
       OR (created_at, id) < (sqlc.narg(after_created_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT @page_limit;
