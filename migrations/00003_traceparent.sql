-- Trace context through the outbox (bonus tracing, T117). The API stores the request's W3C
-- traceparent with the row, like correlation_id, so the relay's publish (possibly much later and
-- in another process) and the delivery join the request's trace. NULL when tracing is off.

-- +goose Up
ALTER TABLE notifications
    ADD COLUMN traceparent text CHECK (traceparent IS NULL OR char_length(traceparent) <= 64);

-- +goose Down
ALTER TABLE notifications DROP COLUMN traceparent;
