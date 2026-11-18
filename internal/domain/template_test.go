package domain

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseTemplateAllowsOnlyTextAndFields(t *testing.T) {
	ok := []struct {
		body string
		vars []string
	}{
		{"Hi {{.Name}}, your code is {{.Code}}", []string{"Code", "Name"}},
		{"plain text, no variables", []string{}},
		{"{{.B}} {{.A}} {{.B}} {{ .A }}", []string{"A", "B"}}, // sorted and unique
		{"{{- .Name -}} trimmed", []string{"Name"}},
	}
	for _, tc := range ok {
		vars, err := ParseTemplate(tc.body)
		if err != nil {
			t.Fatalf("%q: unexpected error %v", tc.body, err)
		}
		if !slices.Equal(vars, tc.vars) {
			t.Fatalf("%q: vars %v, want %v", tc.body, vars, tc.vars)
		}
	}

	rejected := []string{
		"{{range .Items}}x{{end}}",
		"{{if .On}}x{{end}}",
		"{{with .A}}x{{end}}",
		"{{.Name | printf \"%q\"}}",  // pipeline
		"{{printf \"%s\" .Name}}",    // function call
		"{{len .Name}}",              // builtin call
		"{{template \"x\"}}",         // nested template
		"{{define \"x\"}}y{{end}}hi", // definition
		"{{block \"x\" .}}y{{end}}",
		"{{.User.Name}}",     // nested field
		"{{.}}",              // dot
		"{{$x := .A}}{{$x}}", // variables
		"{{/* comment */}}",
		"{{.Name",           // syntax error
		"{{.Name}} {{end}}", // stray end
	}
	for _, body := range rejected {
		_, err := ParseTemplate(body)
		var verr *ValidationError
		if !errors.As(err, &verr) || len(verr.Fields) != 1 || verr.Fields[0].Field != "body" {
			t.Fatalf("%q: want a ValidationError on body, got %v", body, err)
		}
	}
}

func TestNewTemplateValidatesNameAndBody(t *testing.T) {
	tpl, err := NewTemplate("t-1", "otp", "Hi {{.Name}}, code {{.Code}}", t0)
	if err != nil {
		t.Fatal(err)
	}
	if tpl.ID != "t-1" || tpl.Name != "otp" || !slices.Equal(tpl.Variables, []string{"Code", "Name"}) || !tpl.CreatedAt.Equal(t0.Truncate(time.Microsecond)) {
		t.Fatalf("unexpected template %+v", tpl)
	}

	_, err = NewTemplate("t-2", "", strings.Repeat("x", MaxTemplateBody+1), t0)
	var verr *ValidationError
	if !errors.As(err, &verr) || len(verr.Fields) != 2 {
		t.Fatalf("want name and body errors together, got %v", err)
	}
	if _, err := NewTemplate("t-3", strings.Repeat("n", 101), "x", t0); !errors.As(err, &verr) || verr.Fields[0].Field != "name" {
		t.Fatalf("name over 100 chars: %v", err)
	}
	if _, err := NewTemplate("t-4", "blank", "   ", t0); !errors.As(err, &verr) || verr.Fields[0].Field != "body" {
		t.Fatalf("blank body: %v", err)
	}
}

func TestRenderNamesEveryMissingVariable(t *testing.T) {
	tpl, err := NewTemplate("t-1", "otp", "Hi {{.Name}}, your code is {{.Code}} ({{.Expiry}})", t0)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tpl.Render(map[string]string{"Name": "Ada", "Code": "1234", "Expiry": "5 min", "Extra": "ignored"})
	if err != nil || got != "Hi Ada, your code is 1234 (5 min)" {
		t.Fatalf("render = %q, %v", got, err)
	}

	_, err = tpl.Render(map[string]string{"Name": "Ada"})
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("want a ValidationError, got %v", err)
	}
	var fields []string
	for _, f := range verr.Fields {
		fields = append(fields, f.Field)
	}
	if !slices.Equal(fields, []string{"variables.Code", "variables.Expiry"}) {
		t.Fatalf("missing variables %v, want both Code and Expiry named", fields)
	}

	// Values are inserted verbatim: no HTML escaping, no further template evaluation.
	got, err = tpl.Render(map[string]string{"Name": "<b>{{.Code}}</b>", "Code": "1", "Expiry": "x"})
	if err != nil || got != "Hi <b>{{.Code}}</b>, your code is 1 (x)" {
		t.Fatalf("render = %q, %v", got, err)
	}
}

// Templates are channel-agnostic: the rendered text still has to fit the channel.
func TestRenderedContentStillFacesChannelLimits(t *testing.T) {
	tpl, err := NewTemplate("t-1", "long", "Code: {{.Code}}", t0)
	if err != nil {
		t.Fatal(err)
	}
	content, err := tpl.Render(map[string]string{"Code": strings.Repeat("9", 200)})
	if err != nil {
		t.Fatal(err)
	}
	if ValidateContent(ChannelSMS, content) == "" {
		t.Fatal("a 206-character render must fail the sms limit")
	}
	if ValidateContent(ChannelEmail, content) != "" {
		t.Fatal("the same render fits an email")
	}
}
