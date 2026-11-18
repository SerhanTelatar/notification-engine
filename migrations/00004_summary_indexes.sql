-- Metrics summary (bonus, T116, research R16): the summary reads the last 5 minutes of sends,
-- failures and attempts. Partial/ordered indexes keep those window scans proportional to the
-- window, not to the table's history.

-- +goose Up
CREATE INDEX notifications_sent_at_idx ON notifications (sent_at) WHERE sent_at IS NOT NULL;
CREATE INDEX notifications_failed_at_idx ON notifications (failed_at) WHERE failed_at IS NOT NULL;
CREATE INDEX notification_attempts_finished_at_idx ON notification_attempts (finished_at);

-- +goose Down
DROP INDEX notification_attempts_finished_at_idx;
DROP INDEX notifications_failed_at_idx;
DROP INDEX notifications_sent_at_idx;
