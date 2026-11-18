-- Templates (US8, data-model.md § templates, research R12). Templates are channel-agnostic: the
-- channel limits apply to the rendered content, which is what notifications store.

-- +goose Up
CREATE TABLE templates (
    id         uuid        PRIMARY KEY,
    name       text        NOT NULL UNIQUE CHECK (char_length(name) BETWEEN 1 AND 100),
    body       text        NOT NULL CHECK (char_length(body) BETWEEN 1 AND 100000),
    variables  text[]      NOT NULL DEFAULT '{}',   -- derived from the body, sorted
    created_at timestamptz NOT NULL DEFAULT now()
);

-- notifications.template_id existed since 00001 (data-model.md); the FK lands with its table.
ALTER TABLE notifications
    ADD CONSTRAINT notifications_template_id_fkey FOREIGN KEY (template_id) REFERENCES templates (id);

-- +goose Down
ALTER TABLE notifications DROP CONSTRAINT notifications_template_id_fkey;
DROP TABLE templates;
