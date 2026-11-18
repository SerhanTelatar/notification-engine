package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// TemplatesDeps are the template use cases' collaborators.
type TemplatesDeps struct {
	Repo  ports.TemplateRepository
	IDs   ports.IDGenerator
	Clock ports.Clock
	Log   *slog.Logger
}

// Templates implements creating, reading and listing templates (US8).
type Templates struct{ d TemplatesDeps }

// NewTemplates returns the template use cases.
func NewTemplates(d TemplatesDeps) *Templates { return &Templates{d: d} }

// Create validates a template (fields-only, research R12), derives its variables and stores it.
// A taken name is domain.ErrTemplateNameTaken.
func (t *Templates) Create(ctx context.Context, name, body string) (*domain.Template, error) {
	tpl, err := domain.NewTemplate(t.d.IDs.NewID(), name, body, t.d.Clock.Now())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain validation error
	}
	stored, err := t.d.Repo.CreateTemplate(ctx, *tpl)
	if err != nil {
		return nil, fmt.Errorf("store template: %w", err)
	}
	t.d.Log.InfoContext(ctx, "template created", "template_id", stored.ID, "variables", len(stored.Variables))
	return stored, nil
}

// Get returns one template, or domain.ErrTemplateNotFound.
func (t *Templates) Get(ctx context.Context, id string) (*domain.Template, error) {
	tpl, err := t.d.Repo.GetTemplate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}
	return tpl, nil
}

// List returns every template.
func (t *Templates) List(ctx context.Context) ([]domain.Template, error) {
	list, err := t.d.Repo.ListTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return list, nil
}

// templateCache resolves template ids for one request, looking each distinct id up once. An
// unknown id resolves to nil, which buildNotification reports on template_id.
type templateCache struct {
	repo ports.TemplateRepository
	byID map[string]*domain.Template
}

func newTemplateCache(repo ports.TemplateRepository) *templateCache {
	return &templateCache{repo: repo, byID: map[string]*domain.Template{}}
}

func (c *templateCache) resolve(ctx context.Context, req CreateRequest) (*domain.Template, error) {
	if req.TemplateID == nil || *req.TemplateID == "" || req.Content != nil || c.repo == nil {
		return nil, nil // nothing to resolve, or a content/template conflict reported elsewhere
	}
	id := *req.TemplateID
	if tpl, seen := c.byID[id]; seen {
		return tpl, nil
	}
	tpl, err := c.repo.GetTemplate(ctx, id)
	switch {
	case errors.Is(err, domain.ErrTemplateNotFound):
		tpl = nil
	case err != nil:
		return nil, fmt.Errorf("resolve template %s: %w", id, err)
	}
	c.byID[id] = tpl
	return tpl, nil
}
