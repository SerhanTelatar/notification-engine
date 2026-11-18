# notification-engine

An event-driven notification service in Go. It accepts SMS, email and push notifications over a
REST API, stores them durably, and delivers them through an external provider with:
- a rate limit of at most 100 per second per channel, across all workers;
- priority ordering;
- retries with backoff and per-channel circuit breakers;
- crash recovery;
- full observability.

The design is specified in [`specs/001-notification-engine/`](specs/001-notification-engine/)
(spec, plan, research, data model, contracts, tasks), and the key decisions are recorded as ADRs
in [`specs/adr/`](specs/adr/).

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Trade-offs and known limits](#trade-offs-and-known-limits)
- [Bugs caught by tests](#bugs-caught-by-tests)
- [API examples](#api-examples)
- [Configuration](#configuration)
- [Running the tests](#running-the-tests)
- [Validation results](#validation-results)
- [What I'd do with more time](#what-id-do-with-more-time)

## Quick start

You only need Docker with Compose v2.

```bash
cp .env.example .env          # optional: every value in it is the default
docker compose up --build     # postgres, rabbitmq, redis, migrate, api, worker, mockprovider
```

| URL | What |
|-----|------|
| http://localhost:8080/swagger | API docs (Swagger UI over [`api/openapi.yaml`](api/openapi.yaml)) |
| http://localhost:8080/readyz · http://localhost:9090/readyz | API / worker readiness |
| http://localhost:8080/metrics · http://localhost:9090/metrics | Prometheus metrics |

Workers publish their ops port from the range 9090–9099, so `docker compose up --scale worker=2`
works. A single worker usually gets 9090; `docker compose port --index N worker 9090` shows where
each replica landed.
| http://localhost:15672 (guest/guest) | RabbitMQ management UI |
| http://localhost:8090/received | Calls received by the mock provider ([contract](specs/001-notification-engine/contracts/provider.md)) |

`make demo-burst` submits 5,000 mixed-priority SMS in batches. You can watch them drain at about
95/s at the mock provider. To use webhook.site instead of the mock, set
`PROVIDER_URL=https://webhook.site/<uuid>` in `.env` and restart the worker.

## Architecture

Two binaries share one codebase:
- **`api`** accepts and queries notifications.
- **`worker`** runs the outbox relay, the per-channel consumers, the reaper and the backlog
  sampler.

PostgreSQL is the source of truth. RabbitMQ dispatches work, and Redis holds the shared
rate-limiter state.

```mermaid
flowchart LR
    client([Client]) -->|POST /notifications<br/>batches, cancel, list| api[api]
    api -->|"INSERT (tx)<br/>pending / scheduled"| pg[(PostgreSQL<br/>notifications = outbox)]

    subgraph worker [worker]
        relay[Outbox relay<br/>200 ms tick, per channel]
        consumer[Consumers<br/>20 per channel]
        reaper[Reaper<br/>30 s]
        sampler[Backlog sampler<br/>5 s]
    end

    pg -->|"due rows, priority DESC<br/>quota = 1000 − queued"| relay
    relay -->|"publish + confirm<br/>inside the tx"| mq[[RabbitMQ<br/>notifications.sms / email / push<br/>x-max-priority=3]]
    mq --> consumer
    consumer -->|"GCRA permit<br/>100/s per channel"| redis[(Redis)]
    consumer -->|"claim queued→sending<br/>record outcome"| pg
    consumer -->|"POST (via circuit breaker)"| provider([Provider])
    consumer -->|"failed ⇒ reject"| dlq[[notifications.dlq]]
    reaper -->|"stuck sending / queued"| pg
    sampler -->|"notification_backlog<br/>rabbitmq_queue_messages"| prom([Prometheus])
```

The life of a notification:

1. **API.** It validates the request, then inserts a `pending` row, or `scheduled` when
   `scheduled_at` is set. It answers 202. Nothing else happens on the request path, so it stays
   fast (p95 well under 200 ms; see [validation](#validation-results)).
2. **Relay.** Each tick handles each channel inside one transaction:
   - It takes a per-channel advisory lock, so that two workers never relay the same channel at
     once.
   - It selects the rows that are due, highest priority first, up to a quota that keeps at most
     `RELAY_MAX_INFLIGHT_PER_CHANNEL` (1,000) rows `queued`. Anything beyond that waits in
     Postgres, not in the broker.
   - It marks those rows `queued`, publishes them with publisher confirms, then commits.
3. **Consumer.** For each message:
   - It checks the channel's breaker. If the breaker is open, it defers the notification without
     using an attempt.
   - It reserves a GCRA slot in Redis and claims the row with `SELECT … FOR UPDATE` followed by a
     guarded `queued → sending` update. A cancelled row is dropped here.
   - It waits until its slot, calls the provider, and records the attempt with
     `sent | retrying | failed` in one transaction.
4. **Retries.** These are rows with a `next_attempt_at`, which the relay re-publishes when due.
   Failures go to `notifications.dlq` through the dead-letter exchange.
5. **Reaper.** It recovers rows stranded by a crash: `sending` for more than 2 min, and `queued`
   for more than 15 min.

The code is hexagonal: `domain` (stdlib only) ← `ports` ← `app` (use cases) ← `adapters` (http,
postgres, rabbitmq, redis, provider), wired in `cmd/*/main.go`. `depguard` enforces the import
direction in lint. HTTP handlers and DB queries are generated, from
[`api/openapi.yaml`](api/openapi.yaml) by oapi-codegen and from [`sql/queries/`](sql/queries/) by
sqlc. CI fails if the generated code drifts.

## Design decisions

| ADR | Decision |
|-----|----------|
| [0001](specs/adr/0001-rabbitmq-over-kafka-and-postgres-queue.md) | RabbitMQ for work dispatch, rather than Kafka or a Postgres-only queue: native priority queues, per-message acks, DLX |
| [0002](specs/adr/0002-transactional-outbox-in-notifications-table.md) | The `notifications` table is the transactional outbox. The relay publishes with confirms inside its transaction and applies backpressure at 1,000 queued rows per channel |
| [0003](specs/adr/0003-at-least-once-with-idempotency.md) | At-least-once delivery, made effectively once with idempotency keys (payload-hash conflicts → 409) and guarded state transitions |
| [0004](specs/adr/0004-redis-gcra-rate-limiter.md) | Distributed rate limiting with GCRA in a Redis Lua script; calls are paced on their slot; 5% headroom |
| [0005](specs/adr/0005-retry-backoff-and-circuit-breaker.md) | Full-jitter exponential backoff with Retry-After, per-channel circuit breakers, deferral without using attempts, and the `BREAKER_MAX_DEFER_AGE` cap |
| [0006](specs/adr/0006-keyset-pagination.md) | Keyset pagination on `(created_at, id)` with an opaque cursor |
| 0007, 0008 | Reserved for the bonus phases: real-time fan-out over Redis pub/sub (US9) and templates (US8). They aren't implemented yet; see [what I'd do with more time](#what-id-do-with-more-time) |

**Migrations run once, in their own service** (research R14). They are goose SQL files embedded in
the `api` binary. In Compose, a one-shot `migrate` service runs `api migrate up`, and `api` and
`worker` start only after it completes successfully. If both binaries migrated on startup, they
would race. `make migrate` runs the same command against the stack.

**Other notes**:
- Every state change is a guarded `UPDATE … WHERE status IN (…)`, so the state machine in
  [`data-model.md`](specs/001-notification-engine/data-model.md) is enforced by the database, not
  by hoping callers behave.
- All configuration comes from the environment.
- Nothing is registered globally (Prometheus registries and loggers are injected).

## Trade-offs and known limits

- **At-least-once delivery has one duplicate window.**
  - The window: the provider accepts a call, then the worker dies before committing `sent`. The
    reaper moves the row from `sending` to `retrying` after 2 minutes, so the notification may be
    delivered twice.
  - This is the only path to a duplicate. Duplicate submissions, redelivered messages, and the
    relay's own crash window are all absorbed by idempotency keys and guarded transitions.
  - Mitigation: the notification id is sent as the provider's `Idempotency-Key` header, so a
    provider that deduplicates closes the window. ([ADR 0003](specs/adr/0003-at-least-once-with-idempotency.md))
- **Circuit-breaker state is per worker process.**
  - With N workers, up to N trial calls reach a failing provider per cool-down, and up to 5 × N
    failures can happen before every worker has tripped.
  - A shared breaker in Redis would add a round-trip to every provider call, for a goal that is
    "stop hammering", not "exactly one probe".
  - Outages don't consume attempts. A notification deferred for longer than
    `BREAKER_MAX_DEFER_AGE` (1 h) fails as `provider_unavailable` and goes to the DLQ, so nothing
    stays non-terminal forever. ([ADR 0005](specs/adr/0005-retry-backoff-and-circuit-breaker.md))
- **Priority has a prefetch bound.**
  - Ordering is strict in Postgres (the relay) and in RabbitMQ (priority queues).
  - However, messages already prefetched by consumers have left the queue. A new high-priority
    item can therefore wait behind up to `CONSUMER_CONCURRENCY_PER_CHANNEL × workers` (20 per
    worker) lower-priority items that are already in hand.
  - That matches the spec ("a delivery in progress is never pre-empted"). The priority test
    asserts the bound.
- **WebSocket updates are best effort** (bonus US9, not built yet). Status is always correct in
  Postgres and via `GET`. The planned fan-out over Redis pub/sub would drop events while Redis is
  down or a subscriber is slow, and it doesn't replay history. That's why Redis is only *optional*
  for API readiness (`degraded`).
- **Rate-limit headroom: `RATE_LIMIT_HEADROOM=0.05`, kept deliberately.**
  - Slots are spaced 5% wider than `1s / RATE_LIMIT_PER_SECOND`, about 95/s per channel at a limit
    of 100. That's still inside SC-003's 90–100 band.
  - **Gain**: at 100/s a slot is only 10 ms, so a few milliseconds of CPU or network jitter adds an
    extra arrival to a sliding second. Measured at the provider on the Compose stack, the peak was
    101–103 per second without headroom and 96–98 with it. Headroom raises the jitter needed to
    break the limit from about 10 ms to about 50 ms.
  - **Cost**: about 5% of throughput. Every call also starts `RATE_LIMIT_SEND_OFFSET` (50 ms)
    after its slot, to cover the claim.
  - `0` gives exact spacing. It held at 20/s in the integration tests, but not on a loaded host
    at 100/s. ([ADR 0004](specs/adr/0004-redis-gcra-rate-limiter.md))
- **A broker outage pauses delivery, not the worker.** A supervisor redials RabbitMQ with backoff
  and restarts the relay, reaper and consumers on the new connection. Meanwhile the worker answers
  `/healthz` 200 and `/readyz` 503, the API keeps accepting notifications into Postgres, and
  nothing is lost.
- **The rate limiter fails closed.**
  - If Redis is down, nothing is sent: the message is requeued after `LIMITER_MAX_WAIT` and its
    row stays `queued`. Exceeding the provider's limit is worse than waiting.
  - The worker's readiness requires Redis for this reason.

## Bugs caught by tests

The integration tests run against real Postgres, RabbitMQ and Redis (testcontainers). They caught
three bugs that unit tests with fakes could not.

1. **The consumer's claim raced the relay's commit** (fixed in `be59a7b`).
   - **The race**: the relay publishes *inside* its transaction (ADR 0002), so a message can
     reach a consumer before the commit.
   - **The bug**: the original claim was a single guarded `UPDATE … WHERE status='queued'`. Under
     READ COMMITTED, that UPDATE picks its candidate rows from its snapshot, where the row was
     still `pending`. It matched 0 rows without waiting, and the message was dropped. The row was
     then committed as `queued` with no message, and it sat there until the 15-minute reaper
     republished it.
   - **How it was caught**: the delivery integration tests failed whenever a warm consumer beat
     the commit. Each test passed alone and failed when run after another.
   - **The fix**: claim with `SELECT … FOR UPDATE` (no status predicate) and then the guarded
     update, in one transaction. The locking read waits for the relay's row lock and sees the
     committed status.
   - **Guard**: `TestRepoClaimWaitsForAnOpenRelayTransaction` holds a relay transaction open while
     claiming, and it fails without the fix.
   - **Lesson**: real-database tests catch isolation-level assumptions.
2. **The rate limiter could fail open at its deadline** (fixed in `b7bc154`).
   - **The bug**: right after a context deadline passes, `ctx.Err()` can still be nil for a
     moment. `Acquire` returned `(zero, nil)` there, which the deliverer read as a granted permit,
     so a notification could be sent *without* a permit while Redis was down.
   - **How it was caught**: the Redis-outage integration test (traffic cut through a TCP proxy)
     failed 1 run in 10.
   - **The fix**: return `DeadlineExceeded` explicitly. The deliverer also rejects a permit that
     has no slot time.
   - **Guard**: a unit test with a context whose deadline has passed but whose `Err()` is still
     nil. The outage test then passed 15/15.
3. **Exactly spaced permits still bunched at the provider** (fixed in `b7bc154`).
   - **What was seen**: `demo-burst` measured 106 calls in a sliding second against a limit of 100,
     although the permits were exactly 10 ms apart.
   - **The cause**: claim latency between the permit and the call, plus connection setup and host
     jitter.
   - **The fix**: pace each call on its slot plus a constant offset, size the provider connection
     pool, and add the 5% headroom above. The measured peak became 96–98.
   - **Lesson**: measure a limit where the spec measures it, at the provider, not where the code
     enforces it.

## API examples

```bash
API=http://localhost:8080/api/v1

# Create (202). Idempotency-Key makes retries safe; X-Correlation-ID is optional.
curl -s -X POST $API/notifications \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42-sms' -H 'X-Correlation-ID: demo-1' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Your order has shipped","priority":"high"}'

# Schedule for later
curl -s -X POST $API/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"ada@example.com","channel":"email","content":"Reminder","scheduled_at":"2030-01-01T09:00:00Z"}'

# Status with attempt history
curl -s $API/notifications/<id>

# Cancel (only pending / scheduled / queued / retrying; otherwise 409 not_cancellable)
curl -s -X POST $API/notifications/<id>/cancel

# List: filters plus keyset pagination (follow next_cursor)
curl -s "$API/notifications?channel=sms&status=sent&limit=20"
curl -s "$API/notifications?channel=sms&status=sent&limit=20&cursor=<next_cursor>"

# Batch of up to 1,000, with per-item results (202 if at least one is accepted)
curl -s -X POST $API/notifications/batch -H 'Content-Type: application/json' \
  -d '{"items":[{"recipient":"+905551234567","channel":"sms","content":"a"},
               {"recipient":"device-token-1","channel":"push","content":"b","priority":"low"}]}'
curl -s $API/batches/<batch_id>
curl -s -X POST $API/batches/<batch_id>/cancel
```

Errors share one shape: `{"error": {"code", "message", "details": [{"field", "message"}]}}`. A
validation error lists every invalid field at once. The full contract is in
[`api/openapi.yaml`](api/openapi.yaml), served at `/swagger`.

## Configuration

Everything is read from environment variables. [`.env.example`](.env.example) lists every
variable with its default.

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://notify:notify@postgres:5432/notify?sslmode=disable` | PostgreSQL (source of truth) |
| `REDIS_ADDR` | `redis:6379` | Rate-limiter state (required by the worker; optional for the API) |
| `AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/` | RabbitMQ (worker) |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error`; JSON logs |
| `SHUTDOWN_TIMEOUT` | `20s` | Graceful drain on SIGTERM |
| `HTTP_ADDR` | `:8080` | API listen address |
| `MAX_ATTEMPTS` | `5` | Provider calls per notification, stamped at creation |
| `OPS_ADDR` | `:9090` | Worker `/healthz`, `/readyz`, `/metrics` |
| `PROVIDER_URL` | `http://mockprovider:8090/send` | Provider endpoint |
| `PROVIDER_TIMEOUT` | `5s` | Per-call timeout |
| `RATE_LIMIT_PER_SECOND` | `100` | Per channel, across all workers |
| `RATE_LIMIT_SEND_OFFSET` | `50ms` | Calls start at slot + offset (covers the claim) |
| `RATE_LIMIT_HEADROOM` | `0.05` | Slots spaced 5% wider than `1s/limit` |
| `LIMITER_MAX_WAIT` | `30s` | Without a permit by then, the message is requeued |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `5m` | Full-jitter exponential backoff |
| `BREAKER_CONSECUTIVE_FAILURES` | `5` | Transient failures that trip a channel's breaker |
| `BREAKER_COOLDOWN` | `30s` | Open time before one trial call |
| `BREAKER_MAX_DEFER_AGE` | `1h` | Older deferred notifications fail as `provider_unavailable` |
| `RELAY_INTERVAL` | `200ms` | Outbox relay tick |
| `RELAY_BATCH_SIZE` | `500` | Rows per channel per tick |
| `RELAY_MAX_INFLIGHT_PER_CHANNEL` | `1000` | Backpressure: maximum `queued` rows per channel |
| `CONSUMER_CONCURRENCY_PER_CHANNEL` | `20` | Concurrent deliveries per channel per worker |
| `REAPER_INTERVAL` | `30s` | Crash-recovery pass |
| `REAPER_SENDING_TIMEOUT` | `2m` | `sending` older than this is recovered |
| `REAPER_QUEUED_TIMEOUT` | `15m` | `queued` older than this is republished |

## Running the tests

```bash
make test        # unit + integration with -race; integration uses testcontainers (needs Docker)
make test-unit   # unit tests only
make lint        # golangci-lint v2 (pinned image), including depguard layering rules
make gen-check   # generated HTTP/DB code matches api/openapi.yaml and sql/queries
make loadtest    # 10,000 creates with vegeta against the running stack (p50/p95/p99)
```

- **Unit tests** cover the domain and the use cases with in-memory fakes: validation, the state
  machine, backoff, delivery decisions, breaker deferral and its cap, the sampler, and cursors.
- **Integration tests** (`test/integration/`, build tag `integration`) wire the real adapters
  against Postgres, RabbitMQ and Redis containers and a scriptable mock provider. They cover:
  - delivery, retries and the DLQ;
  - relay concurrency and backpressure;
  - the rate limit measured at the provider, including Redis outages;
  - priority and scheduling;
  - cancellation races;
  - breaker isolation;
  - crash recovery;
  - graceful shutdown;
  - correlated logs with no PII;
  - the metrics contract;
  - readiness under broker and Redis outages.
- **CI** (GitHub Actions) runs lint, gen-check, unit and integration tests, and the image build
  on every pull request and on pushes to `main`. Merges to `main` require green CI.

## Validation results

The quickstart scenarios and the load check were run against `docker compose up`. The results are
recorded in
[`specs/001-notification-engine/checklists/validation.md`](specs/001-notification-engine/checklists/validation.md).

## What I'd do with more time

- **The bonus stories**: templates (US8), WebSocket status updates over Redis pub/sub (US9), the
  metrics summary endpoint, OpenTelemetry tracing, and a Grafana dashboard. Their ADRs (0007,
  0008) are reserved.
- **A shared breaker hint**: an "open until" key in Redis that workers read on deferral. This
  would cut the N-probe effect of per-process breakers without putting Redis in every call's path.
- **Partitioned or archived history**: `notifications` and `attempts` grow without bound. Monthly
  partitions plus a retention job would keep the hot indexes small.
- **DLQ tooling**: an endpoint or CLI to inspect and replay dead-lettered notifications, for
  example after a long provider outage.
- **Per-tenant fairness**: rate limits and priority are per channel today. With many API clients,
  one noisy tenant can fill the backlog.
- **Closing the at-least-once window**: with a provider that supports idempotency keys, record the
  provider's response for the key and reconcile `sending` rows by querying the provider instead
  of resending blindly.
- **A long-running soak test** with chaos: broker restarts, network partitions and slow Postgres,
  run for hours rather than the minutes the integration tests take.
