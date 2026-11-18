package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"text/template/parse"
	"time"
	"unicode/utf8"
)

// Template limits (data-model.md § templates).
const (
	MaxTemplateName = 100
	MaxTemplateBody = 100_000
)

const onlyFields = "only text and plain fields like {{.Name}} are allowed"

// NewTemplate validates a template and derives its variables (research R12). Name and body
// problems are reported together as a *ValidationError.
func NewTemplate(id, name, body string, now time.Time) (*Template, error) {
	verr := &ValidationError{}
	switch n := utf8.RuneCountInString(name); {
	case strings.TrimSpace(name) == "":
		verr.Add("name", "must not be empty")
	case n > MaxTemplateName:
		verr.Add("name", fmt.Sprintf("must be at most %d characters (got %d)", MaxTemplateName, n))
	}
	var vars []string
	switch n := utf8.RuneCountInString(body); {
	case strings.TrimSpace(body) == "":
		verr.Add("body", "must not be empty")
	case n > MaxTemplateBody:
		verr.Add("body", fmt.Sprintf("must be at most %d characters (got %d)", MaxTemplateBody, n))
	default:
		v, err := ParseTemplate(body)
		var perr *ValidationError
		if errors.As(err, &perr) {
			verr.Merge("", perr)
		}
		vars = v
	}
	if err := verr.OrNil(); err != nil {
		return nil, err
	}
	return &Template{ID: id, Name: name, Body: body, Variables: vars, CreatedAt: now.UTC().Truncate(time.Microsecond)}, nil
}

// ParseTemplate checks that body uses only text and plain field actions ({{.Name}}) and returns
// the variable names, sorted and unique. Anything else (range/if/with, pipelines, function calls,
// nested or defined templates, nested fields, dot, variables, comments) is a *ValidationError on
// "body". Restricting templates this way lets rendering name every missing variable and rules out
// template injection and unbounded output (research R12).
func ParseTemplate(body string) ([]string, error) {
	nodes, err := parseFields(body)
	if err != nil {
		return nil, err
	}
	vars := []string{}
	for _, n := range nodes {
		if f, ok := n.(*parse.ActionNode); ok {
			vars = append(vars, fieldName(f))
		}
	}
	slices.Sort(vars)
	return slices.Compact(vars), nil
}

// Render substitutes vars into the template. Every variable the template needs but vars lacks is
// named in one *ValidationError (fields "variables.<Name>"); extra variables are ignored. Values
// are inserted verbatim: they are never escaped or evaluated as template text.
func (t *Template) Render(vars map[string]string) (string, error) {
	nodes, err := parseFields(t.Body)
	if err != nil {
		return "", err
	}
	verr := &ValidationError{}
	for _, name := range t.Variables {
		if _, ok := vars[name]; !ok {
			verr.Add("variables."+name, "is required by the template")
		}
	}
	if err := verr.OrNil(); err != nil {
		return "", err
	}
	var b strings.Builder
	for _, n := range nodes {
		switch n := n.(type) {
		case *parse.TextNode:
			b.Write(n.Text)
		case *parse.ActionNode:
			name := fieldName(n)
			v, ok := vars[name]
			if !ok { // the stored variable list disagrees with the body: never render a gap
				return "", NewFieldError("variables."+name, "is required by the template")
			}
			b.WriteString(v)
		}
	}
	return b.String(), nil
}

// parseFields parses body and returns its top-level nodes, which are guaranteed to be text or
// single-field actions.
func parseFields(body string) ([]parse.Node, error) {
	tree := parse.New("template")
	tree.Mode = parse.ParseComments | parse.SkipFuncCheck // surface every construct to the walk below
	trees := map[string]*parse.Tree{}
	if _, err := tree.Parse(body, "", "", trees); err != nil {
		return nil, NewFieldError("body", "invalid template syntax: "+syntaxMessage(err))
	}
	if len(trees) > 1 {
		return nil, NewFieldError("body", onlyFields+"; define/block isn't")
	}
	if tree.Root == nil {
		return nil, nil
	}
	for _, n := range tree.Root.Nodes {
		switch n := n.(type) {
		case *parse.TextNode:
		case *parse.ActionNode:
			if fieldName(n) == "" {
				return nil, NewFieldError("body", fmt.Sprintf("%s; %q isn't", onlyFields, n.String()))
			}
		default:
			return nil, NewFieldError("body", fmt.Sprintf("%s; %q isn't", onlyFields, n.String()))
		}
	}
	return tree.Root.Nodes, nil
}

// fieldName returns X for an action that is exactly {{.X}}, or "" for anything else.
func fieldName(a *parse.ActionNode) string {
	p := a.Pipe
	if p == nil || len(p.Decl) > 0 || len(p.Cmds) != 1 || len(p.Cmds[0].Args) != 1 {
		return ""
	}
	f, ok := p.Cmds[0].Args[0].(*parse.FieldNode)
	if !ok || len(f.Ident) != 1 {
		return ""
	}
	return f.Ident[0]
}

// syntaxMessage drops the "template: template:1:" location prefix from parse errors.
func syntaxMessage(err error) string {
	msg := err.Error()
	if i := strings.LastIndex(msg, ": "); i >= 0 {
		return msg[i+2:]
	}
	return msg
}
