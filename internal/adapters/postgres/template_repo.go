package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres/sqlcgen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// TemplateRepo implements ports.TemplateRepository on PostgreSQL.
type TemplateRepo struct{ q *sqlcgen.Queries }

var _ ports.TemplateRepository = (*TemplateRepo)(nil)

// NewTemplateRepo returns a repository on pool.
func NewTemplateRepo(pool *pgxpool.Pool) *TemplateRepo { return &TemplateRepo{q: sqlcgen.New(pool)} }

// CreateTemplate inserts t. A taken name is domain.ErrTemplateNameTaken.
func (r *TemplateRepo) CreateTemplate(ctx context.Context, t domain.Template) (*domain.Template, error) {
	id, err := uuid.Parse(t.ID)
	if err != nil {
		return nil, fmt.Errorf("template id: %w", err)
	}
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	row, err := r.q.InsertTemplate(ctx, sqlcgen.InsertTemplateParams{ID: id, Name: t.Name, Body: t.Body, Variables: vars, CreatedAt: t.CreatedAt})
	if err != nil {
		if errors.Is(translate(err, ""), ErrUniqueViolation) {
			return nil, fmt.Errorf("template %q: %w", t.Name, domain.ErrTemplateNameTaken)
		}
		return nil, translate(err, "insert template")
	}
	return toTemplate(row), nil
}

// GetTemplate returns one template; an unknown or malformed id is domain.ErrTemplateNotFound.
func (r *TemplateRepo) GetTemplate(ctx context.Context, id string) (*domain.Template, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("template %q: %w", id, domain.ErrTemplateNotFound)
	}
	row, err := r.q.GetTemplate(ctx, u)
	if err != nil {
		if errors.Is(translate(err, ""), domain.ErrNotFound) {
			return nil, fmt.Errorf("template %s: %w", id, domain.ErrTemplateNotFound)
		}
		return nil, translate(err, "get template")
	}
	return toTemplate(row), nil
}

// ListTemplates returns every template, newest first.
func (r *TemplateRepo) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	rows, err := r.q.ListTemplates(ctx)
	if err != nil {
		return nil, translate(err, "list templates")
	}
	out := make([]domain.Template, len(rows))
	for i, row := range rows {
		out[i] = *toTemplate(row)
	}
	return out, nil
}

func toTemplate(r sqlcgen.Template) *domain.Template {
	vars := r.Variables
	if vars == nil {
		vars = []string{}
	}
	return &domain.Template{ID: r.ID.String(), Name: r.Name, Body: r.Body, Variables: vars, CreatedAt: r.CreatedAt}
}
