-- Batch queries (sqlc). A batch row and its notifications are written in one transaction.

-- name: InsertBatch :exec
INSERT INTO batches (id, total_accepted, total_rejected, correlation_id, created_at)
VALUES (@id, 0, 0, @correlation_id, @created_at);

-- name: SetBatchTotals :exec
UPDATE batches SET total_accepted = @total_accepted, total_rejected = @total_rejected WHERE id = @id;

-- name: InsertNotificationsBulk :many
-- One statement for the whole batch (SC-002). sqlc arrays can't carry NULLs, so nullable columns
-- use sentinels that NULLIF turns back into NULL: '' for the idempotency key and template id,
-- the zero time for scheduled_at, '' for template_vars and traceparent. Conflicting idempotency keys are skipped
-- (ON CONFLICT DO NOTHING) and resolved by the repository afterwards.
INSERT INTO notifications (
    id, batch_id, idempotency_key, payload_hash, channel, recipient, content, priority, status,
    attempts, max_attempts, next_attempt_at, scheduled_at, template_id, template_vars,
    correlation_id, traceparent, created_at, updated_at
)
SELECT u.id, @batch_id::uuid, NULLIF(u.idempotency_key, ''), u.payload_hash, u.channel, u.recipient,
       u.content, u.priority, u.status, 0, u.max_attempts, u.next_attempt_at,
       NULLIF(u.scheduled_at, '0001-01-01 00:00:00+00'::timestamptz),
       NULLIF(u.template_id, '')::uuid, NULLIF(u.template_vars, '')::jsonb,
       @correlation_id::text, NULLIF(u.traceparent, ''), @created_at::timestamptz, @created_at::timestamptz
FROM (
    -- Single-argument unnest calls in one SELECT list advance in lockstep (one row per item).
    SELECT unnest(@ids::uuid[])                    AS id,
           unnest(@idempotency_keys::text[])       AS idempotency_key,
           unnest(@payload_hashes::bytea[])        AS payload_hash,
           unnest(@channels::text[])               AS channel,
           unnest(@recipients::text[])             AS recipient,
           unnest(@contents::text[])               AS content,
           unnest(@priorities::smallint[])         AS priority,
           unnest(@statuses::text[])               AS status,
           unnest(@max_attempts::int[])            AS max_attempts,
           unnest(@next_attempt_ats::timestamptz[]) AS next_attempt_at,
           unnest(@scheduled_ats::timestamptz[])   AS scheduled_at,
           unnest(@template_ids::text[])           AS template_id,
           unnest(@template_vars::text[])          AS template_vars,
           unnest(@traceparents::text[])           AS traceparent
) AS u
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id;

-- name: ListByIdempotencyKeys :many
SELECT * FROM notifications WHERE idempotency_key = ANY(@keys::text[]);

-- name: GetBatch :one
SELECT * FROM batches WHERE id = @id;

-- name: CountByStatus :many
SELECT status, count(*)::int AS n FROM notifications WHERE batch_id = @batch_id GROUP BY status;

-- name: ListByBatch :many
SELECT * FROM notifications WHERE batch_id = @batch_id ORDER BY created_at, id;
