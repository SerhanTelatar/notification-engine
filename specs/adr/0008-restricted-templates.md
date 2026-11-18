# ADR 0008: Restricted, fields-only templates rendered at submission

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R12 (US8, FR-028–FR-029)

## Context

Clients want to store message templates with named variables and send by `template_id` plus
`variables` (US8). The spec requires that a send with missing variables is rejected with
**every** missing name listed (US8-AS2). Templates are created through the API, so their bodies
are untrusted input. Notifications are delivered asynchronously, minutes or hours after
submission.

Go's `text/template` is the obvious engine, but:
- it stops at the first missing key, so it can't list every missing variable;
- its full language (`range`, `if`, `with`, pipelines, function calls, nested templates) lets a
  template loop, branch or call functions. Output size then becomes unbounded, and the
  "variables" a template needs can't be known up front.

## Decision

**Templates contain only text and plain field actions `{{.Name}}`.**

`domain.ParseTemplate` parses the body with `text/template/parse`, using `ParseComments` and
`SkipFuncCheck`. Every construct therefore reaches the check, including comments and unknown
functions. It walks the tree and accepts only:
- text nodes;
- actions that are exactly one field with one identifier.

Everything else is a 400 on `body` that names the offending construct: `range`, `if`, `with`,
pipelines, function and builtin calls, `template`, `define` and `block`, nested fields (`.A.B`),
dot, variables, comments, and syntax errors.

**Variables are derived and stored.** The sorted, unique field names are saved in
`templates.variables` when the template is created.

**Rendering walks the same tree; it doesn't execute a template.**
- Before writing anything, `Template.Render` computes `required − provided` and returns every
  missing name at once (`variables.<Name>`).
- Extra variables are ignored.
- Values are inserted verbatim: never escaped, and never evaluated as template text. A value
  containing `{{.Code}}` stays literal.

**Rendering happens at submission, in the API.**
- The rendered text goes through the channel limits like direct content. Templates are
  channel-agnostic, so a render can be valid for email and too long for SMS.
- The notification stores the rendered `content`, plus `template_id` and `template_vars` for
  audit. The worker never sees templates.

**Requests**:
- `content` and `template_id` are mutually exclusive.
- An unknown `template_id` is a 400 on `template_id` (the request is invalid), not a 404.
- Template errors are reported together with the request's other field errors. A batch looks up
  each distinct template once.

**Idempotency**: the payload hash covers `template_id` and `variables`, not the rendered text
(ADR 0003). The same key with the same variables replays; different variables conflict.

**Limits**: names are 1–100 characters and unique (409 `template_name_taken`); bodies are
1–100,000 characters.

## Consequences

- Clients get one complete error per request, rather than fixing variables one at a time.
- **No template injection and bounded output.** A template can't loop or call anything, so the
  output size is at most the body plus the variable values, and the request body limit (10 MB)
  bounds those.
- **Queued notifications are snapshots.** If templates later become editable, changing one can't
  alter what's already queued, retried or scheduled. The same is true today, because templates
  are immutable.
- **Less expressive than a full template language.** There are no conditionals, loops or
  formatting helpers, so clients pre-format values. That's the right trade for notification
  text, and it can be revisited with a vetted function allow-list if needed.
- Rendering in the API adds a template read to a template-based create, which is one indexed
  primary-key lookup.

## Alternatives considered

- **The full `text/template` with `missingkey=error`**: it reports only the first missing key,
  and it can't bound output or work out the required variables for untrusted bodies.
- **Rendering in the worker**: a missing variable would surface as an asynchronous `failed`
  instead of a 400 at submission, which breaks US8-AS2. A template edit would also change
  in-flight messages.
- **A third-party engine (Mustache, Handlebars)**: it would add a dependency for the same
  fields-only subset that the standard library parser already gives us.
