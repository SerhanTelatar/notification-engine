---

description: "Task list for the event-driven notification engine"
---

# Tasks: Event-Driven Notification Engine

**Input**: Design documents from `specs/001-notification-engine/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Required. Constitution V makes failure-path tests mandatory, and the plan's test matrix
lists them. Within each story the test tasks come first and should fail until the implementation
tasks are done.

**Organization**: Tasks are grouped by user story (spec.md, US1–US9) so each story can be built
and checked on its own. Module path: `github.com/SerhanTelatar/notification-engine`.

**Citations**: field constraints are canonical in data-model.md and cited as
`data-model.md <table>.<column>`. Research decisions are cited as `R<n>`.

**Commits**: one task ≈ one Conventional Commit (Constitution IX). All work happens on branch
`001-notification-engine`. Each phase ends with one PR into `main`, merged only when CI (T009) is
green.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on incomplete tasks)
- **[Story]**: The user story the task belongs to (US1…US9)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Repository scaffolding, toolchain, contract promotion and CI (plan phase A).

- [X] T001 Initialise `go.mod`:
  - module `github.com/SerhanTelatar/notification-engine`
  - `go` and `toolchain` directives set to the latest stable Go
  - `tool` directives for oapi-codegen, sqlc, goose and golangci-lint (R1)
  - `.gitignore` for `bin/`, `.env` and coverage files
- [X] T002 [P] Create `Makefile` with targets `gen`, `migrate`, `build`, `up`, `down`, `test`
  (`go test -race -tags integration ./...`), `test-unit`, `lint`, `demo-burst` and `gen-check`
  (`make gen && git diff --exit-code`). Each target must also run inside the `golang` image when
  `DOCKER=1` (R1).
- [X] T003 [P] Create `.golangci.yml`:
  - linters gofmt, goimports, errorlint, wrapcheck, contextcheck, errcheck, revive
  - `depguard` rules so `internal/domain` imports only the stdlib
  - `internal/app` and `internal/ports` must not import `internal/adapters` or
    `internal/platform` (plan, Constitution II)
- [X] T004 [P] Create `.env.example` listing every variable and default from R19, including
  `RELAY_MAX_INFLIGHT_PER_CHANNEL=1000`, `BREAKER_MAX_DEFER_AGE=1h` and `LIMITER_MAX_WAIT=30s`.
- [X] T005 [P] Create a multi-stage `Dockerfile` that builds `api`, `worker` and `mockprovider`
  into one distroless image; the binary is chosen by the container command (plan, Project
  Structure).
- [X] T006 Promote the API contract:
  - move `specs/001-notification-engine/contracts/openapi.yaml` to `api/openapi.yaml`
  - replace the old file with a one-line pointer
  - create `oapi-codegen.yaml` (chi server, strict server, models; exclude tags `realtime` and
    `ops-raw`, R15)
  - run `make gen` to produce `internal/adapters/http/gen/api.gen.go`
- [X] T007 [P] Create `sqlc.yaml`: engine postgresql, schema `migrations/`, queries
  `sql/queries/`, `sql_package: pgx/v5`, output `internal/adapters/postgres/sqlcgen` (R13).
- [X] T008 Create `docker-compose.yml`:
  - postgres:16, rabbitmq:3.13-management and redis:7, each with a healthcheck
  - a one-shot `migrate` service (`api migrate up`)
  - `api` and `worker` with `depends_on` conditions (`service_healthy` /
    `service_completed_successfully`) (R14)
  - ports 8080 (api), 9090 (worker ops) and 15672 (RabbitMQ UI)
- [X] T009 Create `.github/workflows/ci.yml` with jobs `lint` (golangci-lint), `gen-check`,
  `unit` (`make test-unit`), `integration` (`make test`, Docker on ubuntu-latest) and `build`
  (docker build). Required to pass before merge (plan phase A, R18).
- [X] T010 [P] Write `specs/adr/0001-rabbitmq-over-kafka-and-postgres-queue.md` from R3 (context,
  decision, alternatives, consequences).
- [X] T011 [P] Write `specs/adr/0002-transactional-outbox-in-notifications-table.md` from R4,
  including the publish-inside-tx race fix and relay backpressure.
- [X] T012 [P] Write `specs/adr/0003-at-least-once-with-idempotency.md` from R7, including the
  documented residual duplicate window.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: platform, domain core, schema, ports and HTTP/worker skeletons that every story
needs.

**⚠️ CRITICAL**: no user story work starts until this phase is complete.

### Platform

- [X] T013 [P] Implement env config in `internal/platform/config/config.go`:
  - typed `API` and `Worker` structs with the defaults from R19
  - durations parsed with `time.ParseDuration`
  - startup validation that returns all errors at once
  - table-driven tests in `internal/platform/config/config_test.go`
- [X] T014 [P] Implement the slog JSON logger in `internal/platform/logger/logger.go`:
  - `New(service, level)`, `WithCorrelationID(ctx, id)`, `CorrelationID(ctx)`, `From(ctx)`
  - `From(ctx)` adds `correlation_id`, `notification_id` and `batch_id` when they're in ctx
  - fields per `contracts/observability.md` § Logs
  - tests in `internal/platform/logger/logger_test.go`
- [X] T015 [P] Implement the shutdown helpers in `internal/platform/shutdown/shutdown.go`: a
  signal-aware root ctx (SIGTERM/SIGINT) and an errgroup runner with a bounded `SHUTDOWN_TIMEOUT`
  (R17).
- [X] T016 [P] Implement the metrics registry in `internal/platform/metrics/metrics.go`:
  - `NewRegistry()` plus constructors `NewAPIMetrics(reg)` and `NewWorkerMetrics(reg)`
  - define every collector, name, label and bucket listed in `contracts/observability.md`
  - nothing global; the registry is injected
- [X] T017 [P] Implement health checks in `internal/platform/health/health.go`:
  - a `Checker` with required and optional dependencies, 1 s timeout per check
  - JSON output per the `Health` schema in `api/openapi.yaml`
  - a liveness handler that never touches dependencies
  - tests in `internal/platform/health/health_test.go`

### Domain (stdlib only)

- [X] T018 [P] Implement the value objects:
  - `Channel` in `internal/domain/channel.go` (`data-model.md notifications.channel`)
  - `Priority` in `internal/domain/priority.go` (`data-model.md notifications.priority`, high=3,
    normal=2, low=1, default normal)
  - parse/string round-trip tests in `internal/domain/channel_test.go` and
    `internal/domain/priority_test.go`
- [X] T019 [P] Implement the state machine in `internal/domain/status.go`
  (`data-model.md notifications.status`):
  - the 8 statuses, `CanTransition(from, to)`, and the allowed-from sets `CancellableFrom`,
    `DueFrom`, `SendableFrom`
  - `IsTerminal`
  - a table-driven test in `internal/domain/status_test.go` covering all 64 (from, to) pairs
- [X] T020 [P] Define the domain errors in `internal/domain/errors.go`:
  - `ValidationError{Fields []FieldError}`
  - `ErrNotFound`, `ErrInvalidTransition`, `ErrIdempotencyConflict`, `ErrNotCancellable` (carries
    the current status), `ErrTemplateNameTaken`, `ErrTemplateNotFound`
  - all usable with `errors.Is`/`errors.As`
- [X] T021 Implement per-channel validation in `internal/domain/validate.go`:
  - `data-model.md notifications.recipient` (E.164, bare email addr-spec, push token ≤ 4,096
    bytes)
  - `data-model.md notifications.content` (sms ≤ 160 code points, email ≤ 100,000 code points,
    push ≤ 4,000 bytes; non-empty after trimming; content XOR `template_id`, FR-004)
  - every field error is collected, not just the first
  - boundary tests in `internal/domain/validate_test.go`: 160/161, 4000/4001 bytes, multi-byte
    runes, invalid E.164, email with a display name
  - depends on T018 and T020
- [X] T022 Implement the Notification aggregate in `internal/domain/notification.go`:
  - `NewNotification(req, now, maxAttempts, id)` sets `pending`, or `scheduled` only when
    `data-model.md notifications.scheduled_at` is in the future
  - sets `data-model.md notifications.next_attempt_at` (scheduled_at or now) and
    `data-model.md notifications.max_attempts`
  - checks `data-model.md notifications.idempotency_key` is at most 255 chars
  - computes `data-model.md notifications.payload_hash` as SHA-256 of canonical JSON (R7)
  - `TruncateError` for `data-model.md notifications.last_error` (1 KB)
  - tests in `internal/domain/notification_test.go`: past schedule → `pending`; hash stable
    under variable-key order; hash differs on any field change
  - depends on T019 and T021
- [X] T023 Define every port in `internal/ports/ports.go`: `NotificationRepository`,
  `BatchRepository`, `TemplateRepository`, `Publisher`, `Provider` (returns a classified
  `domain.Outcome`, HTTP status, message id and Retry-After), `RateLimiter`, `CircuitBreaker`,
  `StatusBroadcaster`, `MetricsRecorder` (delivery outcomes, retries, dead-letters, e2e latency,
  relay and reaper counters), `Clock`, `IDGenerator`. Every I/O method takes `ctx` first.
- [X] T024 [P] Implement pass-through adapters in `internal/adapters/noop/noop.go`: an
  always-allow `RateLimiter`, an always-closed `CircuitBreaker`, a discarding
  `StatusBroadcaster`, a discarding `MetricsRecorder`, a system `Clock`, and a UUIDv7 `IDGenerator` (`data-model.md
  notifications.id`, `google/uuid`). They're used until US3, US7 and US9 replace them.

### Persistence

- [X] T025 Write `migrations/00001_init.sql` (goose up/down). Create the `notifications`,
  `notification_attempts` and `batches` tables and all their indexes exactly as data-model.md
  defines them.
  - `notifications` columns, each with its CHECK/NOT NULL/UNIQUE/FK constraint:
    - `data-model.md notifications.id`
    - `data-model.md notifications.batch_id` (FK to batches)
    - `data-model.md notifications.idempotency_key` (UNIQUE)
    - `data-model.md notifications.payload_hash`
    - `data-model.md notifications.channel`
    - `data-model.md notifications.recipient`
    - `data-model.md notifications.content`
    - `data-model.md notifications.priority`
    - `data-model.md notifications.status`
    - `data-model.md notifications.attempts`
    - `data-model.md notifications.max_attempts`
    - `data-model.md notifications.next_attempt_at`
    - `data-model.md notifications.scheduled_at`
    - `data-model.md notifications.template_id` (column only; its FK is added in T104)
    - `data-model.md notifications.template_vars`
    - `data-model.md notifications.provider_message_id`
    - `data-model.md notifications.last_error`
    - `data-model.md notifications.correlation_id`
    - `data-model.md notifications.created_at`
    - `data-model.md notifications.updated_at`
    - `data-model.md notifications.sent_at`
    - `data-model.md notifications.cancelled_at`
    - `data-model.md notifications.failed_at`
  - invariant CHECKs: sent ⇔ `sent_at`, `attempts <= max_attempts`, scheduled ⇒ `scheduled_at`.
  - `notification_attempts` columns:
    - `data-model.md notification_attempts.notification_id` (FK, cascade)
    - `data-model.md notification_attempts.attempt_no`, with
      `UNIQUE (notification_id, attempt_no)`
    - `data-model.md notification_attempts.outcome` (CHECK)
    - `data-model.md notification_attempts.http_status`
    - `data-model.md notification_attempts.error`
    - `data-model.md notification_attempts.latency_ms`
    - `data-model.md notification_attempts.started_at`
    - `data-model.md notification_attempts.finished_at`
  - `batches` columns:
    - `data-model.md batches.id`
    - `data-model.md batches.total_accepted`
    - `data-model.md batches.total_rejected`
    - `data-model.md batches.correlation_id`
    - `data-model.md batches.created_at`
  - indexes: `notifications_due_idx` `(channel, priority DESC, next_attempt_at)` partial,
    `notifications_inflight_idx` `(channel, status, updated_at)` partial,
    `notifications_backlog_idx`, `notifications_list_idx`, `notifications_list_filtered_idx`,
    `notifications_batch_idx`
- [X] T026 Embed the migrations and add the migrate subcommand:
  - `migrations/embed.go` (package `migrations`, `embed.FS`)
  - `cmd/api/migrate.go`, a goose provider implementing `api migrate up|down|status`
  - depends on T025
- [X] T027 Implement the Postgres base in `internal/adapters/postgres/db.go`:
  - `pgxpool` constructor with connect/statement timeouts
  - `WithTx(ctx, fn)` helper
  - error translation: unique violation → typed sentinel; `pgx.ErrNoRows` → `domain.ErrNotFound`
  - errors wrapped with `%w`
- [X] T028 [P] Build the testcontainers harness in `test/integration/testenv/testenv.go`
  (build tag `integration`):
  - start postgres:16 (with migrations applied), rabbitmq:3.13 and redis:7 once per package
  - per-test DB truncation
  - an `httptest` mock-provider helper with scriptable responses that records request timestamps

### HTTP and worker skeletons

- [X] T029 Implement the HTTP server in `internal/adapters/http/server.go`:
  - chi router mounting the generated strict handler at `/`
  - a `Server` struct that implements `gen.StrictServerInterface`. Operations not built yet return
    a sentinel `errNotImplemented`, which the strict `ResponseErrorHandlerFunc` maps to 501 with
    code `internal` (501 isn't a declared response in `api/openapi.yaml`)
  - a 10 MiB body limit and the server timeouts from R15
- [X] T030 [P] Implement middleware in `internal/adapters/http/middleware.go`:
  - recover → 500 `internal`
  - correlation id: read `X-Correlation-ID` (≤ 128 chars) or generate UUIDv7, echo it, put it in
    ctx (FR-032)
  - access log with the route pattern
  - request timeout
  - tests in `internal/adapters/http/middleware_test.go`
- [ ] T031 [P] Implement error mapping in `internal/adapters/http/errors.go`:
  - domain errors → `Error` schema (`validation_failed` 400, `not_found` 404,
    `idempotency_conflict` 409, `not_cancellable` 409 with `current_status`,
    `template_name_taken` 409, anything else `internal` 500)
  - log the wrapped error and never put it in the response body (Constitution VIII)
  - request-decoding errors also map to `validation_failed`
  - tests in `internal/adapters/http/errors_test.go`
- [ ] T032 [P] Implement the hand-mounted ops routes in `internal/adapters/http/ops.go`:
  - `/healthz` and `/readyz` using `platform/health`
  - `/metrics` via promhttp on the injected registry
  - `/swagger` (embedded HTML, Swagger UI from a pinned CDN version) and `/openapi.yaml`
    (embedded `api/openapi.yaml`)
- [ ] T033 Wire `cmd/api/main.go`:
  - `serve` (default) and `migrate` subcommands
  - config → logger → pgx pool → metrics → health (postgres required) → server
  - graceful shutdown: `Shutdown`, then close the pool (R17)
  - depends on T026 and T029–T032
- [ ] T034 Wire the skeleton of `cmd/worker/main.go`:
  - config → logger → pool → metrics
  - ops server on `OPS_ADDR` with `/healthz`, `/readyz` and `/metrics`
  - an errgroup that runs components (none yet), with graceful shutdown
  - depends on T013–T017 and T027

**Checkpoint**: `docker compose up` starts the stack; the api returns 501 on product endpoints and
200 on `/healthz`; CI is green.

---

## Phase 3: User Story 1 - Submit a notification and have it delivered reliably (Priority: P1) 🎯 MVP

**Goal**: a single create is validated and accepted with 202, then delivered asynchronously
through the outbox relay → RabbitMQ → consumer → provider. It retries transient failures with
backoff, fails permanent ones, is idempotent, and survives crashes.

**Independent Test**: against the mock provider, create a notification and see it reach `sent`
after exactly one provider call. Script 503s or 400s and see it go to retrying/failed and land in
the DLQ. Kill the worker mid-flight and see nothing lost (quickstart #1–5, #13).

### Tests for User Story 1 ⚠️ (write first)

- [ ] T035 [P] [US1] Write in-memory port fakes in `internal/app/fakes_test.go`: a repository
  that enforces guarded transitions via `domain.CanTransition`, a fake clock, a recording
  publisher, a scriptable provider, a counting limiter, a scriptable breaker, and a recording
  broadcaster.
- [ ] T036 [P] [US1] Write table-driven tests for classification and backoff in
  `internal/domain/retry_test.go`:
  - classification: 2xx success; 429/5xx/timeout/network transient; other 4xx/1xx/3xx permanent
  - full-jitter bounds per attempt, the 5 min cap, the Retry-After floor (seconds and HTTP-date),
    and the max-attempts cut-off (R9)
- [ ] T037 [P] [US1] Write unit tests for the create use case in `internal/app/create_test.go`:
  - valid → 202 data with `pending`
  - scheduled-in-future → `scheduled`
  - validation errors collect every field
  - same key + same hash → original returned with replayed=true
  - same key + different hash → `ErrIdempotencyConflict`
  - a status event is broadcast after commit
- [ ] T038 [P] [US1] Write unit tests for the delivery use case in `internal/app/deliver_test.go`
  (R6):
  - guard 0 rows → ack without a provider call
  - success → `sent` with the message id
  - transient → `retrying` with `next_attempt_at` from backoff and `attempts+1`
  - transient at max attempts → `failed` and the dead-letter signal
  - permanent → `failed` after 1 attempt
  - the limiter is called before the guarded `queued → sending`
  - an attempt row is written with the outcome
  - no permit within `LIMITER_MAX_WAIT` → a release result (consumer `Nack(requeue=true)`); the row
    stays `queued` and the provider isn't called
- [ ] T039 [P] [US1] Write unit tests for the relay in `internal/app/relay_test.go` (R4):
  - `quota = min(batch, max_inflight − queued)` is never negative
  - quota ≤ 0 → channel skipped and the skip metric incremented
  - lock not acquired → channel skipped
  - publish failure → tx rolled back and rows stay due
  - rows picked in `priority DESC, next_attempt_at` order
- [ ] T040 [P] [US1] Write unit tests for the reaper in `internal/app/reaper_test.go` (R5):
  - stale `sending` → `retrying` with an `abandoned` attempt and attempts+1
  - stale `sending` at max attempts → `failed`
  - stale `queued` → republished with no status change
- [ ] T041 [P] [US1] Write the repository integration tests in
  `test/integration/notification_repo_test.go`:
  - idempotent insert (same and different payload)
  - 20 concurrent inserts with the same key → 1 row
  - an illegal guarded transition returns 0 rows
  - the CHECK constraints reject invalid status/priority/attempts
  - two concurrent relay transactions with SKIP LOCKED + advisory lock → each row picked once
- [ ] T042 [P] [US1] Write the create API integration test in `test/integration/api_create_test.go`:
  - 202 + `X-Correlation-ID` echoed
  - 400 with every field error for each channel rule (spec US1-AS3..6)
  - `Idempotency-Key` header replay returns `Idempotent-Replayed: true`
  - header and body keys that differ → 400
  - different payload with the same key → 409
- [ ] T043 [P] [US1] Write the delivery and retry integration tests in
  `test/integration/delivery_test.go`:
  - create → `sent`, with 1 provider call that carries the `Idempotency-Key` header
  - 503 then 202 → `sent` with 2 attempts
  - 503 ×5 → `failed`, with one message in `notifications.dlq`
  - 429 + Retry-After 2 → the second attempt is ≥ 2 s later
  - 400 → `failed` with 1 attempt
  - mock latency 6 s → transient, attempt recorded
  - a 2xx with an unparsable body → `sent` with a null message id
  - 20 concurrent creates with the same idempotency key → after draining, the mock received
    exactly 1 call (SC-005)
- [ ] T044 [P] [US1] Write the relay integration tests in `test/integration/relay_test.go`:
  - a publish that isn't confirmed (exchange deleted) → tx rolled back, rows still `pending`
  - backpressure: 5,000 SMS with `RELAY_MAX_INFLIGHT_PER_CHANNEL=1000` and 2 relays. The
    sampled sms `queued` count is never above 1000, the whole burst is delivered,
    `reaper_recovered_total{kind="queued"}` stays 0, and email isn't blocked
- [ ] T045 [P] [US1] Write the crash-recovery integration tests in
  `test/integration/recovery_test.go`:
  - a stale `sending` row → retried by the reaper and then sent
  - a stale `queued` row without a message → republished and sent
  - the worker ctx cancelled mid-burst, then restarted → every accepted notification reaches a
    terminal status (SC-004)
  - a duplicate message for a `sent` row → acked, no second provider call

### Implementation for User Story 1

- [ ] T046 [US1] Implement `internal/domain/retry.go`:
  - `Outcome` (`data-model.md notification_attempts.outcome`)
  - `Classify(httpStatus, err)`
  - `Backoff(attempt, base, max, retryAfter, rnd)` with full jitter
  - `ShouldFail(attempts, maxAttempts)` (R9)
  - makes T036 pass
- [ ] T047 [US1] Write `sql/queries/notifications.sql` and run `make gen`:
  - `InsertNotification` (`ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`),
    `GetByIdempotencyKey`
  - `RelayTryLock` (`pg_try_advisory_xact_lock(hashtext('relay:'||$1))`), `CountQueued`,
    `PickDue`, `MarkQueued`
  - `StartSending` (guard `status='queued'`), `MarkSent`, `MarkRetrying`, `MarkFailed`,
    `DeferSending`
  - `InsertAttempt`
  - `ReapStaleSending`, `PickStaleQueued`, `TouchQueued`
  - follow the guards in the data-model.md "Write paths" table
- [ ] T048 [US1] Implement `internal/adapters/postgres/notification_repo.go` (`NotificationRepository`):
  - `Create` (on conflict: re-select and compare `data-model.md notifications.payload_hash`)
  - `RelayTx(ctx, channel, fn)`: one tx with lock → count → pick → mark queued → callback publish
    → commit/rollback
  - `StartSending`
  - `ApplyOutcome`: attempt insert + transition in one tx; truncates
    `data-model.md notification_attempts.error` and `data-model.md notifications.last_error` to
    1 KB; sets `data-model.md notifications.sent_at`/`failed_at`
  - the reaper methods
  - makes T041 pass
- [ ] T049 [US1] Implement `internal/app/create.go` (`CreateNotification`):
  - validate via the domain
  - build the aggregate with `IDGenerator`/`Clock`
  - persist the correlation id (`data-model.md notifications.correlation_id`)
  - resolve idempotency (R7)
  - broadcast a status event after commit
  - makes T037 pass
- [ ] T050 [US1] Implement the `createNotification` handler in
  `internal/adapters/http/notifications.go`: map request ↔ domain, merge the `Idempotency-Key`
  header with the body field (mismatch → 400), set the `Idempotent-Replayed` header, return 202.
  Makes T042 pass.
- [ ] T051 [P] [US1] Implement `internal/adapters/rabbitmq/topology.go`: idempotent declaration
  of the exchanges, queues (`x-max-priority=3`, DLX) and bindings exactly as in
  `contracts/events.md` §1.
- [ ] T052 [US1] Implement the confirm publisher in `internal/adapters/rabbitmq/publisher.go`
  (`ports.Publisher`):
  - persistent, priority (from `data-model.md notifications.priority`), `message_id`,
    `mandatory`
  - headers `x-correlation-id` and `traceparent`
  - waits for all deferred confirms with a ctx timeout
  - a returned or nacked message is an error
  - reconnects on channel close
- [ ] T053 [US1] Implement the consumer in `internal/adapters/rabbitmq/consumer.go`:
  - per channel queue, `Qos(prefetch=CONSUMER_CONCURRENCY_PER_CHANNEL)`, N handler goroutines
  - decode the body; a poison message → `Nack(requeue=false)`
  - put the correlation id from the header into ctx
  - ack or nack according to the handler's result
  - stops consuming on ctx cancel and waits for in-flight handlers (R17)
- [ ] T054 [US1] Implement `internal/app/relay.go` (`RelayDue` tick): for each channel run
  `RelayTx` with the backpressure quota from R4, publish via the `Publisher` port, broadcast
  `queued` events, increment `relay_published_total`/`relay_backpressure_skips_total`. Every per-row log line
  carries that row's `correlation_id`; tick-level lines carry a generated `tick_id`. Makes T039
  pass.
- [ ] T055 [US1] Implement the provider client in `internal/adapters/provider/client.go`
  (`ports.Provider`):
  - POST `{to, channel, content}` with the `Idempotency-Key` and `X-Correlation-ID` headers
  - `PROVIDER_TIMEOUT`, no redirects, read at most 64 KiB
  - parse `messageId`; parse Retry-After
  - classify with `domain.Classify` (`contracts/provider.md`)
  - `provider_request_duration_seconds` histogram
- [ ] T056 [US1] Implement `internal/app/deliver.go` (`Deliver` pipeline, R6):
  - breaker check (port)
  - limiter permit (port; errors retried with backoff, fail closed). If no permit is obtained
    within `LIMITER_MAX_WAIT` (default 30 s), return a release result so the consumer does
    `Nack(requeue=true)`; the row stays `queued` (R6). Record `rate_limiter_timeouts_total`.
  - guarded `StartSending` (0 rows → ack)
  - provider call through the breaker
  - `ApplyOutcome` with backoff (`data-model.md notifications.attempts`,
    `data-model.md notifications.max_attempts`, `data-model.md notifications.next_attempt_at`,
    `data-model.md notifications.provider_message_id`)
  - returns ack / dead-letter to the consumer
  - broadcasts transitions
  - makes T038 pass
- [ ] T057 [US1] Implement `internal/app/reaper.go` (`ReapStale` tick, R5): reap stale `sending`
  (an `abandoned` attempt via `data-model.md notification_attempts.outcome`) and republish stale
  `queued` via the `Publisher` port. Increment `reaper_recovered_total`. Makes T040 pass.
- [ ] T058 [P] [US1] Implement the mock provider in `tools/mockprovider/main.go`:
  - `POST /send`, `PUT /control` (optionally scoped per channel), `GET /received`,
    `DELETE /received`, the env defaults and the per-request headers, all per
    `contracts/provider.md`
  - add the `mockprovider` service to `docker-compose.yml`
- [ ] T059 [US1] Wire the worker in `cmd/worker/main.go`:
  - AMQP connection + topology
  - the relay ticker (`RELAY_INTERVAL`)
  - a consumer per channel calling `Deliver`
  - the reaper ticker
  - noop limiter/breaker/broadcaster (T024)
  - worker readiness: postgres + rabbitmq
  - makes T043–T045 pass
- [ ] T060 [US1] Wire the create endpoint into `cmd/api/main.go` with `IDGenerator`, `Clock`,
  `MAX_ATTEMPTS` and the noop broadcaster. Verify quickstart scenarios #1–#5 manually.

**Checkpoint**: MVP. A notification can be created, delivered, retried and failed, and survives a
crash.

---

## Phase 4: User Story 2 - Track delivery status (Priority: P1)

**Goal**: `GET /api/v1/notifications/{id}` returns the full status with attempt history.

**Independent Test**: a notification that failed once and then succeeded shows `sent`,
`attempts: 2` and both attempts with outcome and error; an unknown id → 404 (spec US2-AS1/2). The
batch lookup (US2-AS3) is delivered with US4 in T076.

### Tests for User Story 2 ⚠️

- [ ] T061 [P] [US2] Write unit tests for the get use case in `internal/app/query_test.go`:
  found, with attempts ordered by `attempt_no`; not found → `ErrNotFound`.
- [ ] T062 [P] [US2] Write the integration test in `test/integration/api_get_test.go`:
  - 503 then 202 → GET shows 2 attempts, `http_status` and `error` on the first, the provider
    message id and the sent/created/updated timestamps
  - unknown id → 404 `not_found`
  - malformed uuid → 400

### Implementation for User Story 2

- [ ] T063 [US2] Add `GetNotification` and `ListAttempts` (`ORDER BY attempt_no`) to
  `sql/queries/notifications.sql`, run `make gen`, and implement them in
  `internal/adapters/postgres/notification_repo.go`.
- [ ] T064 [US2] Implement `GetNotification` in `internal/app/query.go`, returning the aggregate
  plus its attempts. Makes T061 pass.
- [ ] T065 [US2] Implement the `getNotification` handler in
  `internal/adapters/http/notifications.go`, mapping to the `NotificationDetail` schema. Makes
  T062 pass.

**Checkpoint**: US1 + US2 together show the full single-notification flow.

---

## Phase 5: User Story 3 - Absorb bursts with rate-limited, prioritised and scheduled delivery (Priority: P1)

**Goal**: at most 100/s per channel across all workers, high → normal → low under a backlog, and
scheduled items held until their time.

**Independent Test**: burst thousands of mixed-priority notifications with 2 workers. The mock's
received timestamps show ≤ limit in any sliding 1 s window, high priority first, and scheduled
items not early (quickstart #11).

### Tests for User Story 3 ⚠️

- [ ] T066 [P] [US3] Write the rate-limit integration test in `test/integration/ratelimit_test.go`
  (real Redis):
  - 2 limiter instances at 20/s for 3 s → at most 20 permits in any sliding 1 s window
  - per-channel keys are independent
  - Redis stopped → `Acquire` returns an error (fail closed)
  - sustained backlog at 20/s for 3 s → at least 54 permits (≥ 90% of the limit; SC-003 lower
    bound)
  - Redis stopped with `LIMITER_MAX_WAIT=2s` → the consumer `Nack(requeue=true)`s the message, the
    row stays `queued`, and it's delivered once Redis is back
  - also an end-to-end run through 2 worker instances checking the mock's timestamps (SC-003)
- [ ] T067 [P] [US3] Write the priority and scheduling integration tests in
  `test/integration/priority_schedule_test.go`:
  - a backlog of 300 low SMS, then 1 high → the high item is within the first
    `concurrency × workers + RELAY_BATCH_SIZE` sends (R10)
  - `scheduled_at` = now + 3 s → status `scheduled`, not received before then, received within
    5 s after (SC-008)
  - `scheduled_at` in the past → `pending`

### Implementation for User Story 3

- [ ] T068 [US3] Implement the GCRA limiter:
  - the Lua script in `internal/adapters/redis/gcra.lua`, embedded
  - `internal/adapters/redis/ratelimiter.go` (`ports.RateLimiter`)
  - key `ratelimit:{channel}`, emission interval `1s / RATE_LIMIT_PER_SECOND`, zero burst, Redis
    `TIME` as the clock
  - `Acquire(ctx, channel)` loops on the returned wait with ctx
  - `rate_limiter_wait_seconds` histogram
  - (R8) makes the first part of T066 pass
- [ ] T069 [US3] Wire the Redis client and limiter into `cmd/worker/main.go`, replacing the noop
  limiter; add Redis to worker readiness. Add a `demo-burst` script in `scripts/demo-burst.sh`
  (5,000 mixed-priority SMS via the batch endpoint, falling back to single creates) referenced by
  the Makefile. Makes T066 and T067 pass.
- [ ] T070 [P] [US3] Write `specs/adr/0004-redis-gcra-rate-limiter.md` from R8: why a token
  bucket violates SC-003, the fail-closed choice, and the alternatives.

**Checkpoint**: all P1 stories are done; burst behaviour is demonstrable.

---

## Phase 6: User Story 4 - Submit notifications in batches (Priority: P2)

**Goal**: `POST /api/v1/notifications/batch` (1–1,000 items, partial success) and
`GET /api/v1/batches/{id}` (status counts + items), which also completes spec US2-AS3.

**Independent Test**: 10 items with 3 invalid → 202, `accepted: 7`, 3 rejected by index; only the
7 are delivered; all invalid → 400 with no batch; 1,001 → 400 (quickstart #6–7).

### Tests for User Story 4 ⚠️

- [ ] T071 [P] [US4] Write unit tests for the batch use case in `internal/app/batch_test.go`:
  - per-item validation, results in request order
  - all invalid → `ValidationError` with `items[i].field` paths and no repository call
  - 0 or 1,001 items → error
  - duplicate idempotency keys inside the batch → later items resolve to the first item's id
  - `GetBatch`: counts per status sum to the accepted total; unknown id → `ErrNotFound`
- [ ] T072 [P] [US4] Write the batch integration test in `test/integration/api_batch_test.go`:
  - 7/3 partial; 1,000 valid acknowledged in < 2 s (SC-002)
  - 1,001 → 400; all invalid → 400 and no `batches` row
  - `GET /batches/{id}` counts sum to the accepted total; unknown → 404

### Implementation for User Story 4

- [ ] T073 [US4] Write `sql/queries/batches.sql`: `InsertBatch` (`data-model.md
  batches.total_accepted`, `data-model.md batches.total_rejected`, `data-model.md
  batches.correlation_id`), a bulk `InsertNotifications` using `unnest(...) ON CONFLICT
  (idempotency_key) DO NOTHING RETURNING` (R13), `GetBatch`, `CountByStatus`, `ListByBatch`. Run
  `make gen`.
- [ ] T074 [US4] Implement `internal/adapters/postgres/batch_repo.go` (`ports.BatchRepository`):
  - the batch row and bulk insert in one tx
  - re-select conflicting keys and compare hashes per item (`data-model.md
    notifications.idempotency_key`, `data-model.md notifications.batch_id`)
- [ ] T075 [US4] Implement `CreateBatch` and `GetBatch` in `internal/app/batch.go`. Each accepted
  item inherits the request correlation id. Broadcast a created event per item. Makes T071 pass.
- [ ] T076 [US4] Implement the `createBatch` and `getBatch` handlers in
  `internal/adapters/http/batches.go`, mapping to the `BatchCreateResult` and `BatchDetail`
  schemas. Makes T072 pass.

**Checkpoint**: batch submission and batch status lookup work on their own.

---

## Phase 7: User Story 5 - Cancel notifications (Priority: P2)

**Goal**: `POST /api/v1/notifications/{id}/cancel` and `POST /api/v1/batches/{id}/cancel`. A
cancelled notification is never delivered.

**Independent Test**: create with `scheduled_at` = now + 10 min, cancel it, cancel it again → 200
both times and never received; cancel a `sent` one → 409 with `current_status` (quickstart #8–9).

### Tests for User Story 5 ⚠️

- [ ] T077 [P] [US5] Write unit tests for the cancel use case in `internal/app/cancel_test.go`:
  - each cancellable status → `cancelled` (`domain.CancellableFrom`)
  - `sending`/`sent`/`failed` → `ErrNotCancellable` with the current status
  - already `cancelled` → success with no change
  - unknown → `ErrNotFound`
  - batch counts: cancelled / already_cancelled / not_cancellable
- [ ] T078 [P] [US5] Write the cancel integration test in `test/integration/cancel_test.go`:
  - cancel a `queued` notification whose message is in RabbitMQ → the consumer acks and the
    provider is never called
  - race ×100: cancel concurrently with the consumer → exactly one wins and no `cancelled` row has
    a provider call (SC-006)
  - batch cancel with mixed states → correct counts
  - unknown id → 404

### Implementation for User Story 5

- [ ] T079 [US5] Add `CancelNotification` (guarded by
  `status IN (pending, scheduled, queued, retrying) RETURNING *`, sets `data-model.md
  notifications.cancelled_at`) and `CancelBatch` (`RETURNING id`, plus counts of the remaining
  statuses) to `sql/queries/notifications.sql`. Run `make gen` and implement them in
  `internal/adapters/postgres/notification_repo.go`.
- [ ] T080 [US5] Implement `Cancel` and `CancelBatch` in `internal/app/cancel.go`:
  - on 0 rows, read the row to decide 404 / 409 / idempotent 200
  - broadcast `cancelled` events
  - makes T077 pass
- [ ] T081 [US5] Implement the `cancelNotification` handler in
  `internal/adapters/http/notifications.go` and the `cancelBatch` handler in
  `internal/adapters/http/batches.go`. The batch cancel needs US4's batches to exist. Makes T078
  pass.

**Checkpoint**: cancellation is safe against in-flight races.

---

## Phase 8: User Story 6 - List and filter notifications (Priority: P2)

**Goal**: `GET /api/v1/notifications` with status/channel/from/to filters, newest first, keyset
cursor, limit default 20 and max 100.

**Independent Test**: 250 matching rows, page size 100 → 3 pages with no repeats or gaps;
filters applied; `limit=101` or a bad cursor → 400 (quickstart #10).

### Tests for User Story 6 ⚠️

- [ ] T082 [P] [US6] Write unit tests for the cursor in `internal/app/cursor_test.go`: encode and
  decode round-trip of `{created_at, id}` as base64url; malformed, truncated or tampered input →
  `ValidationError` on `cursor`.
- [ ] T083 [P] [US6] Write the list integration test in `test/integration/api_list_test.go`:
  - 250 rows → 3 pages, newest first, no duplicates or gaps while concurrent inserts happen
  - `status=failed&channel=email` filter; `from`/`to` range (inclusive/exclusive)
  - an empty result → no `next_cursor`
  - `limit=101` → 400

### Implementation for User Story 6

- [ ] T084 [US6] Implement `internal/app/cursor.go` (opaque keyset cursor) and `List` in
  `internal/app/query.go`, which validates `limit` (1–100, default 20) and fetches `limit+1` to
  derive `next_cursor`. Makes T082 pass.
- [ ] T085 [US6] Add `ListNotifications` to `sql/queries/notifications.sql`:
  - `sqlc.narg` filters
  - `(created_at, id) < ($c, $i)`, `ORDER BY created_at DESC, id DESC`, using
    `notifications_list_idx` / `notifications_list_filtered_idx`
  - run `make gen`, implement it in the repo, and add the `listNotifications` handler in
    `internal/adapters/http/notifications.go`
  - makes T083 pass
- [ ] T086 [P] [US6] Write `specs/adr/0006-keyset-pagination.md` from R13.

**Checkpoint**: list and filter work on their own.

---

## Phase 9: User Story 7 - Operate and monitor the system (Priority: P2)

**Goal**: a per-channel circuit breaker that doesn't consume attempts, the full metric set,
end-to-end correlation ids, real readiness, and verified graceful shutdown.

**Independent Test**: fail SMS at the mock during a burst. The breaker opens, SMS attempts stop
growing, email/push keep flowing, and SMS recovers after the cool-down. The metrics, the
correlation-id grep and readiness behave as in quickstart #12, #14, #15 and #18.

### Tests for User Story 7 ⚠️

- [ ] T087 [P] [US7] Extend `internal/app/deliver_test.go` with the breaker-open path: the
  outcome is a `sending → retrying` deferral with `attempts` unchanged, no attempt row, no limiter
  call, and `next_attempt_at` = now + remaining cool-down (FR-024, R5). A notification older than
  `BREAKER_MAX_DEFER_AGE`, measured from `max(created_at, scheduled_at)`, instead goes
  `sending → failed` with `last_error=provider_unavailable` and the dead-letter signal.
- [ ] T088 [P] [US7] Write the breaker integration test in `test/integration/breaker_test.go`:
  - 5 × 503 on sms → `circuit_breaker_state{channel="sms"}` = 2
  - sms `attempts` frozen while the breaker is open
  - email delivered normally throughout (SC-009)
  - mock fixed → sms sent within one cool-down (test cool-down 2 s)
  - 400 responses never trip it
  - with `BREAKER_MAX_DEFER_AGE=3s` and the mock still failing, an sms notification ends `failed`
    with `last_error=provider_unavailable` and one message in `notifications.dlq`
- [ ] T089 [P] [US7] Write the observability integration test in
  `test/integration/observability_test.go`:
  - capture slog output from the api and the worker → every line for a request with
    `X-Correlation-ID: t-1` carries `t-1`, including the worker lines (SC-011)
  - `/metrics` on both binaries exposes every metric in `contracts/observability.md`
  - stop rabbitmq → worker `/readyz` 503 and `/healthz` 200; stop redis → api `/readyz` 200
    `degraded`
- [ ] T090 [P] [US7] Write the shutdown integration test in `test/integration/shutdown_test.go`:
  cancel the worker root ctx while 50 sends are in flight (mock latency 500 ms) → each row ends
  either `sent` or `queued`, and after a restart all reach `sent` with no duplicate provider calls
  for rows already `sent` (R17). Also send SIGTERM to the api during 200 concurrent creates →
  every request either got 202 with a durable row or failed with no row stored (SC-004,
  US1-AS11).

- [ ] T091 [P] [US7] Write unit tests for the backlog sampler in `internal/app/sampler_test.go`:
  repository counts map to `notification_backlog{channel,priority,status}` gauge values via the
  recorder fake; combinations that vanish are reset to 0; a broker-depth error doesn't stop the
  DB-based gauges.

### Implementation for User Story 7

- [ ] T092 [US7] Implement the per-channel breaker in `internal/adapters/provider/breaker.go`
  (`ports.CircuitBreaker`) with `sony/gobreaker/v2`:
  - `ReadyToTrip` on `BREAKER_CONSECUTIVE_FAILURES` consecutive transient failures
  - `Timeout=BREAKER_COOLDOWN`, `MaxRequests=1`
  - `IsSuccessful` returns true for success and permanent outcomes
  - expose `State()` and the remaining cool-down; update the `circuit_breaker_state` gauge on
    state change (R9)
- [ ] T093 [US7] Implement the breaker-open deferral in `internal/app/deliver.go`:
  `queued → sending → retrying` in one tx via `DeferSending`, without incrementing
  `data-model.md notifications.attempts`. `ErrOpenState`/`ErrTooManyRequests` take the same path.
  Past `BREAKER_MAX_DEFER_AGE`, `DeferSending` instead moves `sending → failed`, setting
  `data-model.md notifications.last_error` to `provider_unavailable` and
  `data-model.md notifications.failed_at`; the consumer then dead-letters the message.
  Wire the breaker in `cmd/worker/main.go` in place of the noop. Makes T087 and T088 pass.
- [ ] T094 [US7] Instrument the metrics:
  - API: an HTTP metrics middleware in `internal/adapters/http/middleware.go`
    (`http_request_duration_seconds` with the chi route pattern); `notifications_created_total` /
    `notifications_rejected_total` in the create and batch handlers
  - worker: `notifications_delivered_total`, `notification_retries_total`,
    `notifications_dead_lettered_total` and `notification_e2e_latency_seconds` emitted from
    `internal/app/deliver.go` through `ports.MetricsRecorder` (defined in T023), with a Prometheus
    implementation in `internal/platform/metrics/recorder.go` replacing the noop recorder
- [ ] T095 [US7] Implement the backlog sampler:
  - `internal/app/sampler.go`, a 5 s tick
  - a `BacklogCounts` query (GROUP BY channel, priority, status on `notifications_backlog_idx`) in
    `sql/queries/notifications.sql`
  - set `notification_backlog{channel,priority,status}`
  - set `rabbitmq_queue_messages{queue}` via passive declare in
    `internal/adapters/rabbitmq/depth.go`
  - (R16)
- [ ] T096 [US7] Finish readiness and correlation:
  - api readiness: postgres required, redis optional/`degraded`
  - worker readiness: postgres, rabbitmq, redis
  - make sure the correlation id flows from create → `data-model.md
    notifications.correlation_id` → AMQP header → consumer ctx → every relay/`Deliver`/reaper log line
  - make sure the recipient and content are never logged
  - files: `cmd/api/main.go`, `cmd/worker/main.go`, `internal/app/deliver.go`
  - makes T089 and T090 pass
- [ ] T097 [P] [US7] Write `specs/adr/0005-retry-backoff-and-circuit-breaker.md` from R9,
  including the per-process breaker trade-off.

**Checkpoint**: all required stories (US1–US7) are done.

---

## Phase 10: Required-scope wrap-up

**Purpose**: per Constitution VII, finish the required features (tested and documented) before
any bonus work.

- [ ] T098 Write `README.md`:
  - setup (`docker compose up`)
  - an architecture diagram (Mermaid, from the plan's pipeline)
  - design decisions linking ADRs 0001–0008, plus a note on the one-shot migrate service (R14)
  - trade-offs: the at-least-once window, the per-process breaker, the priority prefetch bound,
    best-effort WebSocket
  - API curl examples, a configuration table (from `.env.example`), and how to run the tests
  - "what I'd do with more time"
  - (Constitution IX)
- [ ] T099 Write `scripts/loadtest.sh` and a `make loadtest` target:
  - vegeta in Docker (pinned image version) against the running `docker compose` stack
  - 10,000 `POST /api/v1/notifications` with valid, unique SMS payloads
  - report p50/p95/p99, and record the p95 with pass/fail against SC-001 (< 200 ms) in
    `specs/001-notification-engine/checklists/validation.md`
- [ ] T100 Run the required-scope validation (quickstart.md scenarios #1–#15) against
  `docker compose up`. Record the results in `specs/001-notification-engine/checklists/validation.md`
  and fix any gaps by appending tasks.

---

## Phase 11: User Story 8 - Send by template (Priority: P3, bonus)

**Goal**: create, get and list templates, and send by `template_id` + `variables` with every
missing variable named.

**Independent Test**: create `Hi {{.Name}}, your code is {{.Code}}`; sending with both variables
delivers the rendered text; sending without `Code` → 400 naming `Code` (quickstart #16).

### Tests for User Story 8 ⚠️

- [ ] T101 [P] [US8] Write unit tests in `internal/domain/template_test.go`:
  - fields-only parser: `{{.Name}}` OK; `{{range}}`, `{{if}}`, pipelines, function calls and
    nested templates rejected
  - the derived variables are sorted and unique
  - render with missing variables lists all of them; extra variables are ignored
  - rendered content still goes through the channel limits
- [ ] T102 [P] [US8] Write the template integration test in `test/integration/templates_test.go`:
  - create 201 / duplicate name 409 / bad syntax 400
  - GET by id and list
  - send by template → the provider receives the rendered content
  - a missing variable → 400; an unknown template → 400 on `template_id`; `content` together
    with `template_id` → 400
  - a batch item that uses a template

- [ ] T103 [P] [US8] Write unit tests for the template use cases in
  `internal/app/templates_test.go`:
  - create derives sorted variables; a duplicate name → `ErrTemplateNameTaken`
  - create-by-template renders the content, rejects missing variables (all named), rejects
    `content` together with `template_id`, and returns a `ValidationError` on `template_id` for
    an unknown template
  - template variables are included in the payload hash

### Implementation for User Story 8

- [ ] T104 [US8] Write `migrations/00002_templates.sql`:
  - the `templates` table: `data-model.md templates.id`, `data-model.md templates.name` (UNIQUE,
    1–100), `data-model.md templates.body` (≤ 100,000), `data-model.md templates.variables`,
    `data-model.md templates.created_at`
  - add the FK on `data-model.md notifications.template_id`
- [ ] T105 [US8] Implement `internal/domain/template.go`: `ParseTemplate(body)` walks the
  `text/template/parse` tree allowing only text and `.Field` actions (R12); `Variables()`;
  `Render(vars)` with `missingkey=error` that returns every missing name as a `ValidationError`.
  Makes T101 pass.
- [ ] T106 [US8] Write `sql/queries/templates.sql` (`InsertTemplate`, `GetTemplate`,
  `ListTemplates`), run `make gen`, and implement
  `internal/adapters/postgres/template_repo.go` (unique violation →
  `domain.ErrTemplateNameTaken`).
- [ ] T107 [US8] Implement `internal/app/templates.go` (`CreateTemplate`, `GetTemplate`,
  `ListTemplates`) and add template rendering to `internal/app/create.go` and
  `internal/app/batch.go`:
  - resolve the template, render, store the rendered content and `data-model.md
    notifications.template_vars`
  - the variables go into the payload hash
- [ ] T108 [US8] Implement the `createTemplate`, `getTemplate` and `listTemplates` handlers in
  `internal/adapters/http/templates.go`. Makes T102 pass.
- [ ] T109 [P] [US8] Write `specs/adr/0008-restricted-templates.md` from R12: fields-only
  templates, naming every missing variable, injection and runaway-output risks, rendering at
  submission time.

---

## Phase 12: User Story 9 - Receive real-time status updates (Priority: P3, bonus)

**Goal**: `GET /ws?notification_id=|batch_id=` streams a snapshot and then every transition,
including transitions made by the worker.

**Independent Test**: `websocat` subscribed around a create receives a `snapshot`, then
`queued → sending → sent` (quickstart #17).

### Tests for User Story 9 ⚠️

- [ ] T110 [P] [US9] Write hub unit tests in `internal/adapters/ws/hub_test.go`:
  - fan-out by notification id and by batch id
  - unsubscribe on disconnect
  - a full 64-message buffer → the client is closed with 1008
  - shutdown closes with 1001
- [ ] T111 [P] [US9] Write the WebSocket integration test in `test/integration/ws_test.go`:
  - unknown id → 404 before the upgrade; both or neither query params → 400
  - a notification subscription gets the snapshot, then `queued`, `sending`, `sent` published by
    the worker process within 1 s (SC-010)
  - a batch subscription gets events for every item

### Implementation for User Story 9

- [ ] T112 [US9] Implement the Redis broadcaster and subscriber in
  `internal/adapters/redis/status.go`: `ports.StatusBroadcaster` publishes to
  `notification-status` with the payload from `contracts/events.md` §2, best-effort with a
  timeout; `Subscribe(ctx)` returns a channel of events.
- [ ] T113 [US9] Implement the hub in `internal/adapters/ws/hub.go` (indexes by notification and
  batch id, per-client bounded buffer) and the `/ws` handler in `internal/adapters/http/ws.go`:
  - validate params and existence (via `GetNotification`/`GetBatch`) before the upgrade
  - `coder/websocket` accept
  - send the snapshot, then 30 s pings with a 10 s pong timeout (`contracts/events.md` §3)
  - `ws_connections` gauge
  - makes T110 pass
- [ ] T114 [US9] Replace the noop broadcaster with the Redis broadcaster in `cmd/api/main.go` and
  `cmd/worker/main.go`. Start the subscriber → hub loop in the api and mount `/ws`; close clients
  on shutdown. Makes T111 pass.
- [ ] T115 [P] [US9] Write `specs/adr/0007-realtime-fanout-via-redis-pubsub.md` from R11:
  best-effort delivery, snapshot on connect, alternatives (LISTEN/NOTIFY, RabbitMQ fanout).

---

## Phase 13: Polish & Cross-Cutting Concerns (bonus)

- [ ] T116 [P] Implement `GET /api/v1/metrics/summary`:
  - summary queries in `sql/queries/summary.sql`: backlog; sent/failed/retried over the last
    5 min; `percentile_cont` p50/p95/p99 for e2e latency from `notifications` and provider latency
    from `notification_attempts` (`data-model.md notification_attempts.latency_ms`)
  - `internal/app/summary.go`
  - the `getMetricsSummary` handler in `internal/adapters/http/ops.go`
  - an integration test in `test/integration/summary_test.go`
- [ ] T117 [P] Implement OpenTelemetry tracing:
  - `internal/platform/tracing/tracing.go`: OTLP exporter, a no-op when
    `OTEL_EXPORTER_OTLP_ENDPOINT` is empty
  - otelhttp on the router and the provider client
  - `traceparent` inject/extract in `internal/adapters/rabbitmq/publisher.go` and
    `internal/adapters/rabbitmq/consumer.go`
  - add Jaeger to the compose `observability` profile
- [ ] T118 [P] Add Prometheus and Grafana:
  - `deploy/prometheus/prometheus.yml` scraping api:8080 and worker:9090
  - `deploy/grafana/provisioning/` with a datasource and a dashboard
    (`deploy/grafana/dashboards/notifications.json`): backlog by channel/priority, delivery rate
    by outcome, e2e/provider p95, breaker state
  - add prometheus and grafana to the compose `observability` profile
- [ ] T119 Update `README.md` with the bonus features (templates, WebSocket, summary, tracing,
  Grafana). Run the full quickstart.md (#1–#18), `make lint`, `make gen-check` and `make test`;
  make sure CI is green.

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: none. T006 needs T001. T009 needs T002 and T003.
- **Foundational (Phase 2)**: needs Setup, and blocks every story. Internal order: domain
  T018–T020 → T021 → T022; T025 → T026; T029–T032 → T033; T034 after T013–T017 and T027.
- **US1 (Phase 3)**: needs Foundational. It's the MVP and delivers the delivery pipeline every
  later story relies on at runtime.
- **US2 (Phase 4)**: needs Foundational. Its tests need US1's create to make data.
- **US3 (Phase 5)**: needs US1 (relay, consumer, `Deliver`).
- **US4 (Phase 6)**: needs Foundational and US1's repository/create mapping; T076's `getBatch`
  completes US2-AS3.
- **US5 (Phase 7)**: needs US1 (the cancel race uses the consumer); the batch cancel (T081) needs
  US4.
- **US6 (Phase 8)**: needs Foundational only (tests seed through US1's create).
- **US7 (Phase 9)**: needs US1 (`Deliver`, worker wiring).
- **Wrap-up (Phase 10)**: needs US1–US7.
- **US8 / US9 (Phases 11–12)**: need the wrap-up. US8 changes `create.go`/`batch.go` (needs US4).
  US9 needs US1, US2 (lookup before the upgrade) and US4 (batch lookup).
- **Polish (Phase 13)**: needs whichever bonus stories are delivered.

### Story dependency graph

```text
Setup → Foundational → US1 ─┬─► US2
                            ├─► US3
                            ├─► US4 ─► US5 (batch cancel)
                            ├─► US6
                            └─► US7 ─► Wrap-up ─► US8 ─► US9 ─► Polish
```

### Within each story

Tests first (they should fail) → SQL/queries + `make gen` → repository → use case → handler or
worker wiring → checkpoint.

---

## Parallel Opportunities

- **Setup**: T002, T003, T004, T005, T007 and T010–T012 can all run together.
- **Foundational**: T013–T020 (platform + domain value objects/errors), T024, T028, T030–T032.
- **US1**: tests T035–T045 together; then T051 (topology), T055 (provider client) and T058 (mock
  provider) can run alongside the repository work T047–T048.
- **After US1**: US2, US3, US4, US6 and US7 can each be worked on by a different developer; they
  touch mostly different files. Shared files are `sql/queries/notifications.sql` and
  `internal/adapters/postgres/notification_repo.go`, so serialise edits to those.
- **ADRs**: T070, T086 and T097 run in parallel with their story's implementation.

### Parallel example: User Story 1

```bash
# Tests first, all together:
Task: "T036 retry/backoff tests in internal/domain/retry_test.go"
Task: "T037 create use-case tests in internal/app/create_test.go"
Task: "T038 deliver use-case tests in internal/app/deliver_test.go"
Task: "T041 repo integration tests in test/integration/notification_repo_test.go"
Task: "T043 delivery/retry integration tests in test/integration/delivery_test.go"

# Independent adapters together:
Task: "T051 RabbitMQ topology in internal/adapters/rabbitmq/topology.go"
Task: "T055 provider client in internal/adapters/provider/client.go"
Task: "T058 mock provider in tools/mockprovider/main.go"
```

### Parallel example: after the MVP

```bash
Task: "T066 rate-limit integration test (US3)"
Task: "T071 batch use-case tests (US4)"
Task: "T082 cursor tests (US6)"
Task: "T087 breaker-open deliver tests (US7)"
```

---

## Implementation Strategy

### MVP first (US1 only)

1. Phase 1 Setup, with CI green from the first commit.
2. Phase 2 Foundational.
3. Phase 3 US1, then **stop and validate** with quickstart #1–#5 and #13. This is a demoable,
   reliable delivery pipeline.

### Incremental delivery (within the ~12 h budget)

| Step | Scope | Rough budget |
|------|-------|--------------|
| 1 | Setup + Foundational | 2.0 h |
| 2 | US1 (MVP) | 3.0 h |
| 3 | US2 + US3 (all P1) | 1.5 h |
| 4 | US4 + US5 + US6 | 2.0 h |
| 5 | US7 + wrap-up (README, validation) | 2.0 h |
| 6 | Bonus: US8 → US9 → Polish, as time allows | remaining |

The **required-scope cut line** is the end of Phase 10. Nothing in Phases 11–13 starts until
Phase 10's validation passes (Constitution VII).

---

## Notes

- `[P]` = different files with no dependency on incomplete tasks; `[USn]` traces a task to a spec
  user story.
- Generated code (`internal/adapters/http/gen`, `internal/adapters/postgres/sqlcgen`) is
  committed; `make gen-check` in CI catches drift from `api/openapi.yaml` and the SQL.
- Strict-server operations that aren't implemented yet return 501 until their story lands (T029).
- Every guarded transition must check the affected row count. A repository method that ignores it
  is a bug (Constitution III).
- If a story checkpoint fails, fix it before moving on; don't carry broken stories forward.
