-- Templates (US8, data-model.md § templates).

-- name: InsertTemplate :one
-- The unique name is enforced by templates_name_key; the repository maps the violation to
-- domain.ErrTemplateNameTaken.
INSERT INTO templates (id, name, body, variables, created_at)
VALUES (@id, @name, @body, @variables, @created_at)
RETURNING *;

-- name: GetTemplate :one
SELECT * FROM templates WHERE id = @id;

-- name: ListTemplates :many
-- Templates are few and small to list; newest first.
SELECT * FROM templates ORDER BY created_at DESC, id DESC;
