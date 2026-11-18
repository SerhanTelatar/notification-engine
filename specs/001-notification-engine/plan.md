# Implementation Plan: Event-Driven Notification Engine

**Branch**: `001-notification-engine` | **Date**: 2026-11-18 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-notification-engine/spec.md`, plus the technical
direction supplied with `/speckit-plan`. Where that input conflicts with spec.md, spec.md wins; each
such case is marked **(spec override)** in [research.md](./research.md).

## Summary

An HTTP API accepts single and batch notification requests, validates them per channel, and writes
them to PostgreSQL in one transaction. The `notifications` table itself is the transactional
outbox. A relay in the worker publishes due rows to per-channel RabbitMQ priority queues, using
`FOR UPDATE SKIP LOCKED` and publisher confirms inside the same transaction. It applies
backpressure: it only publishes for a channel while that channel has fewer than
`RELAY_MAX_INFLIGHT_PER_CHANNEL` (default 1000) rows in `queued`, so the rest of a burst waits in
Postgres. Consumers then:

- throttle each channel to 100/s across all workers with a Redis GCRA limiter;
- claim each notification with a guarded `queued → sending` update, which is also the atomic
  cancel check;
- call the provider through a per-channel circuit breaker with a 5 s timeout;
- record the attempt and its outcome in one transaction, then ack.

Transient failures go back through the relay with full-jitter exponential backoff, and permanent
or exhausted ones end as `failed` and are dead-lettered. A reaper recovers rows stranded by a
crash. Status changes fan out to WebSocket subscribers through Redis pub/sub. Both binaries expose
Prometheus metrics, slog JSON logs carrying a correlation id, and separate liveness and readiness
endpoints.

## Technical Context

**Language/Version**: Go, latest stable (≥ 1.25), pinned with `go` + `toolchain` in `go.mod`.
Tools are pinned with `tool` directives. Go is **not installed** on the dev machine yet, so every
`make` target can also run in the `golang` image (`DOCKER=1`) (research R1).

**Primary Dependencies**:

- chi v5 + oapi-codegen (strict server)
- pgx/v5 + sqlc, goose
- rabbitmq/amqp091-go, redis/go-redis/v9
- sony/gobreaker/v2, coder/websocket
- prometheus/client_golang, google/uuid
- OpenTelemetry (bonus)

Each one is justified in research R20.

**Storage**:

- PostgreSQL 16 is the source of truth.
- RabbitMQ 3.13 (classic priority queues; 4.x-compatible) carries work.
- Redis 7 holds the rate limiter and the pub/sub channel, and is never a source of truth.

**Testing**:

- `go test -race`, using testify/require.
- Port fakes for domain/app unit tests.
- `testcontainers-go` (postgres, rabbitmq, redis) plus an `httptest` mock provider for
  integration tests, behind the `integration` build tag.
- golangci-lint.
- GitHub Actions CI.

**Target Platform**: Linux containers via docker-compose (local only; cloud deployment is out of
scope).

**Project Type**: web service, as two binaries (`api`, `worker`) plus a test tool
(`mockprovider`).

**Performance Goals**:

- 100 deliveries/s per channel (300/s total), never above 100 in any 1 s window.
- Single create acknowledged in under 200 ms at p95 during a burst of 10k.
- A 1,000-item batch acknowledged in under 2 s.
- Status events reach subscribers in under 1 s.

**Constraints**:

- No dual writes.
- At-least-once delivery, made effectively once by idempotency.
- Every external call has a timeout, and every retry loop has a cap.
- Graceful drain on SIGTERM.
- Roughly a 12-hour build budget.
- One command to run, one command to test.

**Scale/Scope**:

- 9 user stories, 35 FRs.
- About 13 HTTP operations, 3 channels, 8 statuses.
- Backlogs of 10k+ notifications drained at the rate limit.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| # | Principle | How this plan complies | Pre | Post |
|---|-----------|------------------------|-----|------|
| I | Spec-First | spec → plan → tasks. [contracts/openapi.yaml](./contracts/openapi.yaml) becomes `api/openapi.yaml`, and the strict server is generated from it. Eight ADRs are planned (list below). | ✅ | ✅ |
| II | Hexagonal (NON-NEGOTIABLE) | `internal/domain` imports only the stdlib (text/template/parse, net/mail and similar; no infrastructure). `internal/app` depends on `internal/ports` interfaces. Adapters implement the ports. Wiring is only in `cmd/*/main.go`. No globals; Prometheus collectors go on an injected registry. A `depguard` lint rule enforces the import direction. | ✅ | ✅ |
| III | Reliability | The API writes only to the DB; the outbox relay publishes inside the transaction (R4). Idempotency comes from a unique key plus the status guard (R7). Every transition is a guarded `UPDATE … WHERE status = ANY(...)` (data-model). Provider, AMQP, Redis and DB calls all have timeouts; retries are capped; the terminal states are `sent`, `failed` (plus DLQ) and `cancelled`; the reaper covers crashes. | ✅ | ✅ |
| IV | Observability | slog JSON with `correlation_id` everywhere. Metrics in [contracts/observability.md](./contracts/observability.md), with every latency as a histogram. Both binaries have `/healthz` and `/readyz`, and readiness checks real dependencies. | ✅ | ✅ |
| V | Test Discipline | Table-driven unit tests with port fakes. Testcontainers for adapters. A matrix of failure-path tests (below). `make test` runs everything; CI gates merges. | ✅ | ✅ |
| VI | Go Quality | gofmt + golangci-lint (errorlint, wrapcheck, contextcheck, depguard). `%w` wrapping. `ctx` as the first parameter. `panic` only in main. Graceful shutdown (R17). Each dependency is justified (R20). | ✅ | ✅ |
| VII | Simplicity | Required features come first (phases A–C). Templates, WebSocket, metrics summary, tracing and Grafana come after (phase D). Two binaries; the relay lives in the worker. No speculative abstractions; there's one adapter per port. | ✅ | ✅ |
| VIII | Security & Config | Config comes only from env vars, with defaults in `.env.example` (R19). Validation happens at the boundary. One consistent error shape that never exposes internals (errors are logged, a generic `internal` code is returned). Recipient and content are not logged. | ✅ | ✅ |
| IX | Delivery Hygiene | Conventional Commits, one task per commit. The README sections required by the constitution are tracked as a task. | ✅ | ✅ |

**Gate result**: PASS. The one justified deviation is listed under Complexity Tracking.

**Planned ADRs** (`specs/adr/`, written during implementation from the research entries):

1. `0001-rabbitmq-over-kafka-and-postgres-queue.md` (R3)
2. `0002-transactional-outbox-in-notifications-table.md` (R4)
3. `0003-at-least-once-with-idempotency.md` (R7)
4. `0004-redis-gcra-rate-limiter.md` (R8)
5. `0005-retry-backoff-and-circuit-breaker.md` (R9)
6. `0006-keyset-pagination.md` (R13)
7. `0007-realtime-fanout-via-redis-pubsub.md` (R11, written in US9)
8. `0008-restricted-templates.md` (R12, written in US8)

## Reconciliation with the planning input (spec.md wins)

| Input said | Changed to | Why (spec ref) | Research |
|------------|-----------|----------------|----------|
| Breaker open → status `retrying` straight from `queued` | `queued → sending → retrying` in one tx, no attempt consumed, no rate-limit permit used | FR-012 forbids `queued → retrying`; FR-024 | R5, R6 |
| Reaper resets stale `queued`/`sending` to `retrying` | stale `sending` → `retrying`/`failed`; stale `queued` is republished without a status change | FR-012 | R5 |
| Consumer loads the row, then skips it if not `queued` | the consumer's *first* DB step is the guarded update; the relay publishes inside its tx | Without this, a message can be lost if the consumer reads before the relay commits (FR-025) | R4 |
| Token bucket 100/s | GCRA with zero burst | SC-003 says "any one-second window"; a 100-capacity bucket allows ~200 | R8 |
| Only 202 counts as success | any 2xx counts as success | FR-019 says "accepted response"; webhook.site returns 200 | R9 |
| Rate-limit permit taken after `sending` | permit taken before `sending` | keeps items cancellable while throttled (FR-013/014) | R6 |
| `queue_depth{channel}` from RabbitMQ | `notification_backlog{channel,priority,status}` from the DB (the RabbitMQ gauge is kept as well) | FR-031 asks for depth per channel **and priority** | R16 |
| Templates have a `channel` column | channel-agnostic; only `{{.Var}}` allowed | spec assumption; FR-029 "naming each" missing variable | R12 |
| Migrations run on startup | a one-shot `migrate` compose service runs before api/worker | avoids two binaries racing on migrations | R14 |
| Retry-After not mentioned | honoured as the minimum delay | FR-021 | R9 |
| Worker ops endpoints not listed | worker has its own `/metrics`, `/healthz`, `/readyz` | Constitution IV ("every component") | R16 |
| Relay publishes every due row | per-channel cap on `queued` rows (`RELAY_MAX_INFLIGHT_PER_CHANNEL`, default 1000), enforced exactly with a per-channel advisory lock | without a cap, a large burst waits in RabbitMQ longer than the reaper's 15-min threshold and is republished as duplicates (FR-026) | R4 |
| CI workflow in the bonus phase | CI in phase A | Constitution V: "CI must be green before merge" has to hold from the first commit | R18 |

## Project Structure

### Documentation (this feature)

```text
specs/001-notification-engine/
├── plan.md              # this file
├── research.md          # Phase 0: decisions R1–R20
├── data-model.md        # Phase 1: tables, state machine, guards
├── quickstart.md        # Phase 1: run + validation scenarios
├── contracts/
│   ├── openapi.yaml     # HTTP contract draft (moves to api/openapi.yaml)
│   ├── events.md        # AMQP messages, Redis status events, WebSocket protocol
│   ├── provider.md      # outbound provider contract + mock provider controls
│   └── observability.md # metric names/labels, log fields, health semantics
├── checklists/requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks)
specs/adr/               # ADRs 0001–0006
```

### Source Code (repository root)

```text
cmd/
├── api/main.go                 # wiring; subcommands: serve (default), migrate up|down|status
└── worker/main.go              # wiring: relay, consumers, reaper, sampler, ops server
internal/
├── domain/                     # pure; stdlib only
│   ├── notification.go         # aggregate, constructors, invariants
│   ├── status.go               # Status, CanTransition, allowed-from sets
│   ├── channel.go priority.go  # value objects
│   ├── validate.go             # per-channel recipient/content rules
│   ├── template.go             # parse (fields-only), variables, render
│   ├── retry.go                # Classify(outcome), Backoff(attempt, retryAfter, rand)
│   └── errors.go               # ErrValidation (with field errors), ErrNotFound, ErrInvalidTransition, ErrIdempotencyConflict …
├── ports/                      # interfaces owned by app
│   └── ports.go                # NotificationRepository, TemplateRepository, Publisher, Provider,
│                               # RateLimiter, CircuitBreaker, StatusBroadcaster, MetricsRecorder,
│                               # Clock, IDGenerator
├── app/                        # use cases; depend on domain + ports only
│   ├── create.go batch.go cancel.go query.go templates.go
│   ├── relay.go                # RelayDue (tick)
│   ├── deliver.go              # Deliver(message) pipeline (R6)
│   ├── reaper.go               # ReapStale
│   └── fakes_test.go           # in-memory port fakes for unit tests
├── adapters/
│   ├── http/                   # strict-server impl, error mapping, middleware, swagger, /ws mount
│   │   └── gen/                # oapi-codegen output (generated, committed)
│   ├── postgres/               # repositories on sqlc
│   │   └── sqlcgen/            # sqlc output (generated, committed)
│   ├── rabbitmq/               # topology, confirm publisher, consumer
│   ├── redis/                  # GCRA limiter (Lua), status broadcaster/subscriber
│   ├── provider/               # HTTP client, response classification, gobreaker wrapper
│   ├── noop/                   # pass-through limiter/breaker/broadcaster/recorder until real adapters land
│   └── ws/                     # hub: subscriptions by notification/batch id, backpressure
└── platform/
    ├── config/                 # env parsing + defaults + validation
    ├── logger/                 # slog JSON, ctx correlation helpers
    ├── metrics/                # registry + collectors (injected)
    ├── health/                 # liveness/readiness checkers
    ├── shutdown/               # signal ctx + errgroup helpers
    └── tracing/                # OTel setup (bonus, no-op when disabled)
api/openapi.yaml                # canonical contract
migrations/                     # goose SQL (embedded)
sql/queries/                    # sqlc query files
sqlc.yaml  oapi-codegen.yaml
test/integration/               # testcontainers suites (build tag: integration)
test/integration/testenv/       # shared container harness + scriptable mock provider
scripts/                        # demo-burst.sh, loadtest.sh (vegeta in Docker)
tools/mockprovider/             # mock provider binary + Dockerfile
deploy/prometheus/ deploy/grafana/
.github/workflows/ci.yml
Dockerfile                      # multi-stage; one image, entrypoint picks api|worker
docker-compose.yml  Makefile  .env.example  .golangci.yml  README.md
```

**Structure Decision**: a single Go module with a hexagonal layout. `internal/ports` is its own
package so that adapters can implement interfaces without importing `internal/app`, which keeps the
dependency direction `adapters → ports → domain` and `app → ports → domain`, enforced by depguard.
Generated code is committed so the repo builds without the generators installed. `make gen` plus a
CI check (`git diff --exit-code`) prevents drift from the contract.

## Delivery pipeline (reference)

```text
API ──tx──► notifications(pending|scheduled)            ◄── cancel: guarded UPDATE → cancelled
                 │ relay tick (200ms), per channel, in one tx:
                 │   try advisory lock(channel) ─ held elsewhere ⇒ skip channel
                 │   quota = min(batch, MAX_INFLIGHT(1000) − count(queued))
                 │     quota ≤ 0 ⇒ skip (backlog stays in Postgres)
                 │   SELECT due LIMIT quota FOR UPDATE SKIP LOCKED
                 │   → UPDATE queued → publish+confirm → COMMIT
                 ▼
     RabbitMQ notifications.{sms,email,push} (x-max-priority=3)
                 │ consumer (20/channel/worker)
                 ├─ breaker open? → sending→retrying (no attempt) → ack
                 │     held > BREAKER_MAX_DEFER_AGE (1h) ⇒ sending→failed
                 │     (provider_unavailable) → DLQ
                 ├─ GCRA permit (Redis, 100/s/channel global)
                 │     no permit within LIMITER_MAX_WAIT (30s) ⇒ nack(requeue), row stays queued
                 ├─ guarded queued→sending (0 rows ⇒ ack & drop)   ◄── atomic cancel check
                 ├─ provider POST (5s timeout, via breaker)
                 └─ tx: attempt row + sent | retrying(next_attempt_at=backoff) | failed → ack
                                                     failed ⇒ nack(no requeue) → DLX → dlq
reaper (30s): sending>2m → retrying/failed ; queued>15m → republish
status change ─► Redis PUBLISH ─► every API instance ─► WebSocket subscribers
```

## Implementation phases

These are the order for `/speckit-tasks` (Constitution VII: required features first).

**Delivery workflow**: all work happens on branch `001-notification-engine`. Each phase of
tasks.md ends with one PR into `main`, and that PR is merged only when CI is green
(Constitution V, IX).

- **A. Foundation**
  - module, Makefile, Dockerfile, compose, `.env.example`, config, logger, shutdown
  - promote the OpenAPI file and run codegen
  - migrations and sqlc
  - domain (state machine, validation, retry policy) with unit tests
  - GitHub Actions CI (`.github/workflows/ci.yml`): lint → unit → integration → build images. It
    starts gating merges here (Constitution V).
  - ADRs 0001–0003
- **B. Ingest (US1 create, US2 query, US4 batch, US5 cancel, US6 list)**
  - postgres repositories with integration tests
  - app use cases with unit tests
  - HTTP handlers, error mapping, correlation middleware
- **C. Delivery (US1 delivery, US3, US7 core)**
  - relay, AMQP topology and publisher
  - provider client and breaker
  - GCRA limiter
  - consumer/Deliver pipeline
  - reaper
  - metrics, readiness
  - mock provider
  - failure-path integration tests
  - ADRs 0004–0006
  - README
  - load check for SC-001 (`scripts/loadtest.sh`)
- **D. Bonus (in order)**
  - scheduled-delivery tests (the behaviour already comes from `next_attempt_at`)
  - templates (US8)
  - WebSocket (US9)
  - `/metrics/summary`
  - OTel tracing
  - Grafana dashboard

## Test matrix (Constitution V)

| Area | Unit (fakes) | Integration (testcontainers) |
|------|--------------|------------------------------|
| State machine | every (from,to) pair against the transition table | guarded UPDATE returns 0 rows on an illegal transition |
| Validation | per-channel boundaries (160/161 chars, 4000/4001 bytes, multibyte, E.164, email) | 400 field-level response through HTTP |
| Idempotency | payload-hash canonicalisation | same key + same payload → same id; different payload → 409; 20 concurrent same-key → 1 row, 1 provider call |
| Batch | per-item validation, intra-batch duplicate keys | 7/3 partial accept; all invalid → 400, no batch; 1,001 → 400; 1,000 in < 2 s |
| Pagination | cursor encode/decode, malformed cursor | 250 rows, 3 pages, no gaps or duplicates under concurrent inserts |
| Cancel | allowed-from sets | cancel queued → never sent; cancel vs consumer race ×100 → exactly one winner; batch cancel counts |
| Relay | quota maths: `min(batch, max_inflight − queued)`, never negative | two relays + SKIP LOCKED → each row published once; rollback when a publish isn't confirmed |
| Relay backpressure | — | burst of 5,000 SMS with `RELAY_MAX_INFLIGHT_PER_CHANNEL=1000` and 2 relays: sampled `queued` count for sms is never above 1000; the whole burst is delivered; `reaper_recovered_total{kind="queued"}` stays 0; email isn't blocked by the sms cap |
| Retry | backoff bounds, full jitter, Retry-After floor, max-attempts cut-off | 503 ×5 → failed + DLQ; 503 then 202 → sent with 2 attempts; 429 + Retry-After respected |
| Permanent | classification table | 400 → failed, 1 attempt, breaker not tripped |
| Timeout | — | mock latency 6 s → transient, attempt recorded |
| Breaker | open path consumes no attempt; past `BREAKER_MAX_DEFER_AGE` → `failed` (`provider_unavailable`) | 5 × 503 on sms opens it; sms attempts frozen while open; email unaffected; recovers after cool-down; max defer age 3 s → `failed` + DLQ |
| Rate limit | limiter timeout → release result | 2 worker instances, limit 20/s, 3 s: max in any sliding 1 s window ≤ 20 and total ≥ 54 (≥ 90%); Redis down with `LIMITER_MAX_WAIT=2s` → nack(requeue), row stays `queued`, delivered after Redis returns |
| Priority | relay ordering | backlog of low items, then high: the high item is within the first `concurrency × workers + RELAY_BATCH_SIZE` sends |
| Scheduled | status chosen at create time | not sent before `scheduled_at`; sent within 5 s after |
| Crash recovery | reaper decisions | stale `sending` → retrying with an abandoned attempt; stale `queued` republished; nothing lost after a worker restart |
| Templates *(D)* | fields-only parser, missing-variables list, render + limits | 400 naming every missing variable |
| WebSocket *(D)* | hub fan-out, overflow close | subscriber gets snapshot + transitions from a status change made by the worker |
| Shutdown | — | worker SIGTERM during in-flight work → no message lost, rows end consistent; api SIGTERM during concurrent creates → each request is either 202 with a durable row or failed with no row |
| Load (SC-001) | — | `scripts/loadtest.sh`: vegeta, 10k creates, p95 recorded in `checklists/validation.md` |

## Complexity Tracking

| Deviation | Why needed | Simpler alternative rejected because |
|-----------|------------|--------------------------------------|
| `/ws`, `/metrics` and `/swagger` are documented in `openapi.yaml` but hand-mounted (tags excluded from codegen) instead of generated (Constitution I) | A WebSocket upgrade needs to hijack the connection, which the strict server doesn't allow. `/metrics` is Prometheus text served by `promhttp`. `/swagger` is a static page. | Generating a non-strict server just for these would mean two handler styles for the whole API. Leaving them out of the contract would hide part of the API surface from reviewers. |

The other differences from the planning input aren't constitution deviations. They're listed in
the reconciliation table above and recorded in research.md and the ADRs.
