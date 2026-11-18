package httpapi

import (
	"context"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/http/gen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
)

// CreateTemplate stores a fields-only template (US8-AS1): 201, 400 on syntax, 409 on a taken name.
func (s *Server) CreateTemplate(ctx context.Context, req gen.CreateTemplateRequestObject) (gen.CreateTemplateResponseObject, error) {
	if s.templates == nil {
		return nil, errNotImplemented
	}
	if req.Body == nil {
		return nil, domain.NewFieldError("body", "is required")
	}
	tpl, err := s.templates.Create(ctx, req.Body.Name, req.Body.Body)
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	return gen.CreateTemplate201JSONResponse(toAPITemplate(tpl)), nil
}

// GetTemplate returns one template, or 404.
func (s *Server) GetTemplate(ctx context.Context, req gen.GetTemplateRequestObject) (gen.GetTemplateResponseObject, error) {
	if s.templates == nil {
		return nil, errNotImplemented
	}
	tpl, err := s.templates.Get(ctx, req.Id.String())
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	return gen.GetTemplate200JSONResponse(toAPITemplate(tpl)), nil
}

// ListTemplates returns every template, newest first.
func (s *Server) ListTemplates(ctx context.Context, _ gen.ListTemplatesRequestObject) (gen.ListTemplatesResponseObject, error) {
	if s.templates == nil {
		return nil, errNotImplemented
	}
	list, err := s.templates.List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // domain errors are mapped to the contract by the error handler
	}
	items := make([]gen.Template, len(list))
	for i := range list {
		items[i] = toAPITemplate(&list[i])
	}
	return gen.ListTemplates200JSONResponse{Items: items}, nil
}

func toAPITemplate(t *domain.Template) gen.Template {
	return gen.Template{Id: parseUUID(t.ID), Name: t.Name, Body: t.Body, Variables: t.Variables, CreatedAt: t.CreatedAt}
}
