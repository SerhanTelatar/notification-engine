-- Metrics summary over a recent window (bonus, T116, research R16). Each query is bounded by
-- @since through an index from migration 00004.

-- name: SummarySent :many
-- Sends in the window with end-to-end latency percentiles: sent_at − max(created_at, scheduled_at).
SELECT s.channel,
       count(*)::int                                            AS sent,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY s.e2e_ms)::float8 AS p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY s.e2e_ms)::float8 AS p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY s.e2e_ms)::float8 AS p99
FROM (
    SELECT channel,
           (extract(epoch FROM sent_at - greatest(created_at, coalesce(scheduled_at, created_at))) * 1000)::float8 AS e2e_ms
    FROM notifications
    WHERE sent_at >= @since::timestamptz
) AS s
GROUP BY s.channel;

-- name: SummaryFailed :many
SELECT channel, count(*)::int AS failed
FROM notifications
WHERE failed_at >= @since::timestamptz
GROUP BY channel;

-- name: SummaryAttempts :many
-- Provider calls in the window. A transient or abandoned attempt that wasn't the notification's
-- last allowed one is a transition to retrying (notification_retries_total).
SELECT n.channel,
       count(*)::int                                                               AS attempts,
       (count(*) FILTER (WHERE a.outcome = 'success'))::int                        AS successes,
       (count(*) FILTER (WHERE a.outcome IN ('transient', 'abandoned')
                           AND a.attempt_no < n.max_attempts))::int                AS retried,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY a.latency_ms)::float8            AS p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY a.latency_ms)::float8            AS p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY a.latency_ms)::float8            AS p99
FROM notification_attempts AS a
JOIN notifications AS n ON n.id = a.notification_id
WHERE a.finished_at >= @since::timestamptz
GROUP BY n.channel;
