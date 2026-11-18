package app

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// memTemplates is an in-memory ports.TemplateRepository with a unique name, like Postgres.
type memTemplates struct {
	mu   sync.Mutex
	byID map[string]domain.Template
	gets int
}

func newTemplateRepo() *memTemplates { return &memTemplates{byID: map[string]domain.Template{}} }

var _ ports.TemplateRepository = (*memTemplates)(nil)

func (r *memTemplates) CreateTemplate(_ context.Context, t domain.Template) (*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.byID {
		if existing.Name == t.Name {
			return nil, domain.ErrTemplateNameTaken
		}
	}
	r.byID[t.ID] = t
	return &t, nil
}

func (r *memTemplates) GetTemplate(_ context.Context, id string) (*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gets++
	t, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrTemplateNotFound
	}
	return &t, nil
}

func (r *memTemplates) ListTemplates(context.Context) ([]domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.Template, 0, len(r.byID))
	for _, t := range r.byID {
		out = append(out, t)
	}
	return out, nil
}

func newTemplatesUC(repo *memTemplates) *Templates {
	return NewTemplates(TemplatesDeps{Repo: repo, IDs: &seqIDs{}, Clock: newClock(), Log: quietLog})
}

func fieldNames(err error) []string {
	var verr *domain.ValidationError
	if !errors.As(err, &verr) {
		return nil
	}
	var out []string
	for _, f := range verr.Fields {
		out = append(out, f.Field)
	}
	return out
}

func TestCreateTemplateDerivesVariablesAndRejectsDuplicates(t *testing.T) {
	repo := newTemplateRepo()
	uc := newTemplatesUC(repo)
	ctx := context.Background()

	tpl, err := uc.Create(ctx, "otp", "Hi {{.Name}}, your code is {{.Code}}")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tpl.Variables, []string{"Code", "Name"}) || tpl.ID == "" {
		t.Fatalf("unexpected template %+v", tpl)
	}
	if got, err := uc.Get(ctx, tpl.ID); err != nil || got.Name != "otp" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if list, err := uc.List(ctx); err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}

	if _, err := uc.Create(ctx, "otp", "other"); !errors.Is(err, domain.ErrTemplateNameTaken) {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := uc.Create(ctx, "bad", "{{range .X}}{{end}}"); !slices.Equal(fieldNames(err), []string{"body"}) {
		t.Fatalf("bad syntax: %v", err)
	}
	if _, err := uc.Get(ctx, "00000000-0000-7000-8000-999999999999"); !errors.Is(err, domain.ErrTemplateNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
}

type templateFixture struct {
	create    createFixture
	templates *memTemplates
	tplID     string
}

func newTemplateFixture(t *testing.T) templateFixture {
	t.Helper()
	f := templateFixture{create: newCreateFixture(), templates: newTemplateRepo()}
	tpl, err := newTemplatesUC(f.templates).Create(context.Background(), "otp", "Hi {{.Name}}, your code is {{.Code}}")
	if err != nil {
		t.Fatal(err)
	}
	f.tplID = tpl.ID
	f.create.uc = NewCreator(CreatorDeps{
		Repo: f.create.repo, Templates: f.templates, IDs: &seqIDs{}, Clock: f.create.clock,
		Events: f.create.events, Log: quietLog, MaxAttempts: 5,
	})
	return f
}

func (f templateFixture) request(vars map[string]string) CreateRequest {
	id := f.tplID
	return CreateRequest{Recipient: "+905551234567", Channel: "sms", TemplateID: &id, Variables: vars, CorrelationID: "c-1"}
}

func TestCreateByTemplateRendersAndStoresVariables(t *testing.T) {
	f := newTemplateFixture(t)
	vars := map[string]string{"Name": "Ada", "Code": "1234"}
	res, err := f.create.uc.Create(context.Background(), f.request(vars))
	if err != nil {
		t.Fatal(err)
	}
	n := res.Notification
	if n.Content != "Hi Ada, your code is 1234" {
		t.Fatalf("content %q: the rendered text is what gets stored and sent", n.Content)
	}
	if n.TemplateID == nil || *n.TemplateID != f.tplID || n.TemplateVars["Code"] != "1234" {
		t.Fatalf("template id/vars not kept for audit: %+v", n)
	}
}

func TestCreateByTemplateValidation(t *testing.T) {
	f := newTemplateFixture(t)
	ctx := context.Background()

	// Every missing variable is named in one error.
	_, err := f.create.uc.Create(ctx, f.request(nil))
	if got := fieldNames(err); !slices.Equal(got, []string{"variables.Code", "variables.Name"}) {
		t.Fatalf("missing variables: %v (%v)", got, err)
	}

	// content together with template_id.
	req := f.request(map[string]string{"Name": "Ada", "Code": "1"})
	req.Content = strp("also content")
	if got := fieldNames(mustErr(f.create.uc.Create(ctx, req))); !slices.Equal(got, []string{"content"}) {
		t.Fatalf("content + template_id: %v", got)
	}

	// An unknown template is a validation error on template_id, not a 404.
	unknown := "00000000-0000-7000-8000-999999999999"
	req = f.request(map[string]string{"Name": "Ada"})
	req.TemplateID = &unknown
	if got := fieldNames(mustErr(f.create.uc.Create(ctx, req))); !slices.Equal(got, []string{"template_id"}) {
		t.Fatalf("unknown template: %v", got)
	}

	// The rendered text still has to fit the channel.
	if got := fieldNames(mustErr(f.create.uc.Create(ctx, f.request(map[string]string{"Name": strings.Repeat("a", 200), "Code": "1"})))); !slices.Equal(got, []string{"content"}) {
		t.Fatalf("rendered content over the sms limit: %v", got)
	}
	if len(f.create.repo.rows) != 0 {
		t.Fatal("nothing may be stored for a rejected request")
	}
}

func mustErr(_ any, err error) error { return err }

// The variables are part of the payload hash: same key + same variables replays, same key +
// different variables conflicts.
func TestTemplateVariablesAreInThePayloadHash(t *testing.T) {
	f := newTemplateFixture(t)
	ctx := context.Background()
	key := "k-tpl"
	req := f.request(map[string]string{"Name": "Ada", "Code": "1"})
	req.IdempotencyKey = &key

	first, err := f.create.uc.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.create.uc.Create(ctx, req)
	if err != nil || !again.Replayed || again.Notification.ID != first.Notification.ID {
		t.Fatalf("same variables must replay: %+v %v", again, err)
	}
	req.Variables = map[string]string{"Name": "Ada", "Code": "2"}
	if _, err := f.create.uc.Create(ctx, req); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("different variables must conflict: %v", err)
	}
}

func TestBatchItemsCanUseTemplates(t *testing.T) {
	templates := newTemplateRepo()
	tpl, err := newTemplatesUC(templates).Create(context.Background(), "otp", "Code {{.Code}}")
	if err != nil {
		t.Fatal(err)
	}
	repo := newBatchRepo()
	uc := NewBatcher(BatcherDeps{Repo: repo, Templates: templates, IDs: &seqIDs{}, Clock: newClock(),
		Events: &recBroadcaster{}, Log: quietLog, MaxAttempts: 5})
	id := tpl.ID
	items := []CreateRequest{
		{Recipient: "+905551234567", Channel: "sms", TemplateID: &id, Variables: map[string]string{"Code": "1"}},
		{Recipient: "+905551234568", Channel: "sms", TemplateID: &id},
		{Recipient: "+905551234569", Channel: "sms", TemplateID: &id, Variables: map[string]string{"Code": "3"}},
	}
	res, err := uc.CreateBatch(context.Background(), BatchRequest{Items: items, CorrelationID: "c-b"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 || res.Rejected != 1 || res.Items[0].Notification.Content != "Code 1" {
		t.Fatalf("unexpected result %+v", res)
	}
	if errs := res.Items[1].Errors; len(errs) != 1 || errs[0].Field != "variables.Code" {
		t.Fatalf("item 1 errors %v, want variables.Code", errs)
	}
	if templates.gets != 1 {
		t.Fatalf("template looked up %d times, want once per distinct id", templates.gets)
	}
}
