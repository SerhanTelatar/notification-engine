<!--
Sync Impact Report
==================
Version change: (unversioned template) → 1.0.0
Bump rationale: initial ratification; all placeholders replaced with project-specific governance.

Modified principles (template slot → new title):
- [PRINCIPLE_1_NAME] → I. Spec-First, Always
- [PRINCIPLE_2_NAME] → II. Hexagonal Boundaries (NON-NEGOTIABLE)
- [PRINCIPLE_3_NAME] → III. Reliability by Design
- [PRINCIPLE_4_NAME] → IV. Observability Is a Feature
- [PRINCIPLE_5_NAME] → V. Test Discipline

Added sections:
- Principles VI. Go Code Quality, VII. Simplicity Within Budget,
  VIII. Security & Config, IX. Delivery Hygiene (template expanded from 5 to 9 principles)
- Technology & Architecture Constraints (was [SECTION_2_NAME])
- Development Workflow & Quality Gates (was [SECTION_3_NAME])

Removed sections: none

Deferred TODOs: none

Note: this report is review scratch material; remove it before committing the constitution.
-->

# notification-engine Constitution

## Core Principles

### I. Spec-First, Always

- No code is written without an approved spec, plan and task (`/speckit-specify` →
  `/speckit-plan` → `/speckit-tasks`). Every commit that changes behaviour MUST trace to a task.
- `api/openapi.yaml` is the single API contract. HTTP server types and routing MUST be generated
  from it; hand-written code MUST NOT diverge from the contract. Contract changes happen in the
  YAML first, then code is regenerated.
- Every significant design decision (queue choice, outbox strategy, retry policy, idempotency
  model, etc.) MUST have a short ADR in `specs/adr/` covering context, decision, alternatives
  considered and consequences.

**Rationale**: reviewers judge reasoning as much as code; specs and ADRs make that reasoning
explicit and keep the implementation honest to the contract.

### II. Hexagonal Boundaries (NON-NEGOTIABLE)

- `internal/domain` MUST NOT import infrastructure packages (database drivers, `net/http`,
  AMQP, Redis clients, or any adapter package). It contains entities, value objects, state
  machine rules and domain errors only.
- Application use cases MUST depend only on port interfaces. Adapters implement ports.
  Replacing the notification provider, message queue or database MUST require changes only in
  adapters and wiring, never in domain or application code.
- No global mutable state (no package-level clients, singletons or `init()` side effects).
  All dependencies are constructed and injected explicitly in `cmd/*/main.go`.

**Rationale**: clean boundaries make the core logic testable without infrastructure and prove
the design can evolve without rewrites.

### III. Reliability by Design

- PostgreSQL is the single source of truth. The API MUST write only to the database; it MUST NOT
  publish to the broker directly. Messages reach the queue via a transactional outbox and relay
  (no dual writes).
- Delivery is at-least-once, made effectively-once through idempotency: database unique
  constraints (e.g. idempotency keys) plus a status check before every provider send.
  Exactly-once delivery MUST NOT be claimed anywhere in code or docs.
- Every state transition MUST be a guarded conditional update (`UPDATE ... WHERE status IN
  (...)`) with the affected row count checked. Illegal transitions MUST be impossible at the
  storage layer, not merely unlikely in application code.
- Every external call (provider, broker, cache, database) MUST have a timeout. Every retry loop
  MUST have a cap and backoff. Every failure path MUST end in a terminal status or a dead-letter
  queue; no message may be silently dropped or retried forever.

**Rationale**: a notification system is judged by what happens when things fail; these rules
make failure behaviour deterministic and auditable.

### IV. Observability Is a Feature

- Logging MUST use structured JSON via `log/slog`. Every log line on a request or message path
  MUST carry a `correlation_id` propagated from API ingress through outbox, queue and worker.
- Every component (api, worker, relay) MUST expose Prometheus metrics. Latencies MUST be
  recorded as histograms, never as averages or gauges.
- Liveness and readiness MUST be separate endpoints. Readiness MUST check real dependencies
  (database, broker, cache as applicable); liveness MUST NOT.

**Rationale**: an operator must be able to answer "what happened to notification X?" and
"is the system healthy?" without attaching a debugger.

### V. Test Discipline

- Domain and application logic MUST have table-driven unit tests. Test doubles are limited to
  fakes of port interfaces; types we own MUST NOT be mocked beyond that.
- Adapters MUST have integration tests against real PostgreSQL, RabbitMQ and Redis using
  `testcontainers-go`.
- Failure paths are mandatory test cases: provider 5xx, provider timeout, 429 rate limiting,
  non-retryable 4xx, cancel-vs-send races and duplicate submissions. A feature with only
  happy-path tests is not done.
- `make test` MUST run the full suite with one command. CI MUST be green before merge.

**Rationale**: reliability claims (Principle III) are only credible when each failure mode is
demonstrated by a test.

### VI. Go Code Quality

- Code MUST be `gofmt`-formatted and pass `golangci-lint` with the repository config.
- Errors MUST be wrapped with context using `%w` and inspected with `errors.Is` / `errors.As`.
  `panic` MUST NOT be used outside `main` (startup misconfiguration only).
- `context.Context` MUST be the first parameter of every function performing I/O and MUST be
  honoured for cancellation, deadlines and shutdown.
- On SIGTERM each process MUST shut down gracefully: stop accepting new work, drain in-flight
  requests/messages within a bounded timeout, then close connections.
- Prefer the standard library. A third-party dependency is added only when it removes real
  complexity, and the reason is noted in the plan or an ADR.

**Rationale**: idiomatic, predictable Go is what senior reviewers expect and what keeps the
codebase maintainable.

### VII. Simplicity Within Budget

- Required features MUST be implemented, tested and documented before any bonus feature starts.
- YAGNI: no speculative abstractions, plugin systems or generic frameworks without a current
  requirement. The deployable topology is limited to the `api` / `worker` split (plus the outbox
  relay where the plan places it); no further microservices.

**Rationale**: the ~12h budget rewards a complete, solid core over a broad, fragile surface.

### VIII. Security & Config

- No secrets, credentials or tokens are committed. All configuration comes from environment
  variables with documented defaults in `.env.example`.
- All input MUST be validated at the boundary (HTTP handlers, message consumers) before reaching
  application code.
- Errors MUST be returned in one consistent JSON error shape (defined in the OpenAPI contract)
  and MUST NOT leak internals such as stack traces, SQL or driver messages.

**Rationale**: safe defaults and a uniform error contract are baseline professional hygiene.

### IX. Delivery Hygiene

- Commits MUST follow Conventional Commits. One task ≈ one commit; the history MUST read as a
  coherent story from scaffold to finished system.
- The README MUST cover: setup via `docker-compose up`, an architecture diagram, key design
  decisions (linking ADRs), trade-offs, API usage examples, and what would be done with more
  time.

**Rationale**: the repository itself is the deliverable; reviewers read history and docs as
closely as code.

## Technology & Architecture Constraints

- **Language**: Go (current stable release), modules enabled.
- **Datastore**: PostgreSQL — source of truth for notifications, status history and the outbox.
- **Broker**: RabbitMQ — transport for work dispatch, retries and dead-lettering.
- **Cache / coordination**: Redis — only for concerns that do not need durability (e.g. rate
  limiting); never a source of truth.
- **Contract**: OpenAPI 3 at `api/openapi.yaml`; server code generated from it.
- **Layout**: `cmd/<binary>/main.go` for wiring; `internal/domain` (pure), `internal/app`
  (use cases + ports), `internal/adapters/*` (infrastructure). Exact package names are fixed in
  the plan but MUST preserve this dependency direction: adapters → app → domain.
- **Runtime**: the full stack MUST start locally with `docker-compose up`.

## Development Workflow & Quality Gates

1. **Spec gate**: feature spec approved, ambiguities resolved (`/speckit-clarify`) before planning.
2. **Plan gate**: the plan's Constitution Check MUST pass; any deviation is recorded in the
   plan's Complexity Tracking table or an ADR with justification.
3. **Task gate**: work proceeds task by task; each task lands as one Conventional Commit.
4. **Merge gate**: `gofmt`, `golangci-lint` and `make test` (unit + integration) all pass in CI.
5. **Done gate**: required failure-path tests exist (Principle V), metrics and logs are wired
   (Principle IV), and README/ADRs are updated for any changed behaviour or decision.

## Governance

- This constitution overrides convenience and supersedes ad-hoc practices. When a rule and a
  shortcut conflict, the rule wins unless a deviation is explicitly justified in the plan's
  Complexity Tracking section or in an ADR.
- Every plan and review MUST verify compliance with these principles. Principle II is
  NON-NEGOTIABLE: deviations from it are not accepted, only amendments to the constitution.
- **Amendments**: proposed via `/speckit-constitution`, recorded with a Sync Impact Report, and
  committed as `docs: amend constitution to vX.Y.Z (...)`. Dependent specs/plans MUST be
  re-checked after an amendment.
- **Versioning** (semantic): MAJOR for removing or redefining a principle in a
  backward-incompatible way; MINOR for adding a principle/section or materially expanding
  guidance; PATCH for clarifications and wording fixes.

**Version**: 1.0.0 | **Ratified**: 2026-11-18 | **Last Amended**: 2026-11-18
