-- Initial schema (data-model.md). PostgreSQL is the single source of truth (Constitution III).
-- Every constraint below is cited in specs/001-notification-engine/data-model.md.

-- +goose Up
CREATE TABLE batches (
    id             uuid        PRIMARY KEY,
    total_accepted integer     NOT NULL CHECK (total_accepted >= 0),
    total_rejected integer     NOT NULL CHECK (total_rejected >= 0),
    correlation_id text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  uuid        PRIMARY KEY,
    batch_id            uuid        REFERENCES batches (id),
    idempotency_key     text        UNIQUE
                                    CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
    payload_hash        bytea       NOT NULL CHECK (octet_length(payload_hash) = 32),
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           text        NOT NULL CHECK (recipient <> ''),
    content             text        NOT NULL CHECK (content <> ''),
    priority            smallint    NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 3),
    status              text        NOT NULL CHECK (status IN (
                                        'pending', 'scheduled', 'queued', 'sending',
                                        'sent', 'retrying', 'failed', 'cancelled')),
    attempts            integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts        integer     NOT NULL CHECK (max_attempts >= 1),
    next_attempt_at     timestamptz NOT NULL,
    scheduled_at        timestamptz,
    template_id         uuid,       -- FK to templates added with US8 (migration 00002)
    template_vars       jsonb,
    provider_message_id text,
    last_error          text        CHECK (octet_length(last_error) <= 1024),
    correlation_id      text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz,
    cancelled_at        timestamptz,
    failed_at           timestamptz,

    CONSTRAINT notifications_sent_iff_sent_at  CHECK ((status = 'sent') = (sent_at IS NOT NULL)),
    CONSTRAINT notifications_attempts_le_max   CHECK (attempts <= max_attempts),
    CONSTRAINT notifications_scheduled_has_time CHECK (status <> 'scheduled' OR scheduled_at IS NOT NULL)
);

CREATE TABLE notification_attempts (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      integer     NOT NULL CHECK (attempt_no >= 1),
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz NOT NULL,
    latency_ms      integer     NOT NULL CHECK (latency_ms >= 0),
    http_status     integer     CHECK (http_status BETWEEN 100 AND 599),
    outcome         text        NOT NULL CHECK (outcome IN ('success', 'transient', 'permanent', 'abandoned')),
    error           text        CHECK (octet_length(error) <= 1024),

    CONSTRAINT notification_attempts_unique_no UNIQUE (notification_id, attempt_no)
);

-- Outbox relay: due rows per channel in priority order (research R4).
CREATE INDEX notifications_due_idx
    ON notifications (channel, priority DESC, next_attempt_at)
    WHERE status IN ('pending', 'scheduled', 'retrying');

-- Relay backpressure count of queued rows per channel, and the reaper (research R4, R5).
CREATE INDEX notifications_inflight_idx
    ON notifications (channel, status, updated_at)
    WHERE status IN ('queued', 'sending');

-- Backlog gauge and metrics summary (research R16).
CREATE INDEX notifications_backlog_idx
    ON notifications (channel, priority, status)
    WHERE status NOT IN ('sent', 'failed', 'cancelled');

-- Keyset pagination, newest first (research R13).
CREATE INDEX notifications_list_idx
    ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_list_filtered_idx
    ON notifications (channel, status, created_at DESC, id DESC);

CREATE INDEX notifications_batch_idx
    ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

-- +goose Down
DROP TABLE notification_attempts;
DROP TABLE notifications;
DROP TABLE batches;
