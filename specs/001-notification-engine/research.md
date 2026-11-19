# Phase 0 Research: Event-Driven Notification Engine

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Date**: 2026-11-18

Each entry is: **Decision**, **Rationale**, **Alternatives considered**. Entries marked
**(spec override)** change something in the planning input so it agrees with spec.md, because
spec.md takes precedence. Entries marked **(ADR)** become an ADR in `specs/adr/` during
implementation (Constitution I).

---

## R1. Toolchain and local environment

- **Decision**: Use the latest stable Go (≥ 1.25), pinned with the `go` and `toolchain` directives
  in `go.mod`. Pin the codegen and lint tools (`oapi-codegen`, `sqlc`, `goose`, `golangci-lint`)
  as `tool` directives in `go.mod` and run them with `go tool <name>`. Each `make` target can also
  run inside the `golang` Docker image (`make DOCKER=1 gen`), because Go isn't installed on the dev
  machine right now.
- **Rationale**: no global tool installs are needed, every contributor and CI gets the same
  versions, and reviewers can run everything with only Docker installed.
- **Alternatives**: a `tools.go` blank-import file (the old pattern, replaced by `tool`
  directives); asking reviewers to install the tools by hand (more friction, version drift).
- **Amendment (T001)**: only `oapi-codegen` (v2.8.2) is a `tool` directive. `sqlc` (1.31.1) and
  `golangci-lint` (v2.15.0) run from their pinned official Docker images, both in the Makefile
  and in CI. golangci-lint's own docs advise against installing it through `go.mod`, because its
  dependencies would mix with the module's. sqlc needs cgo and would pull a large dependency tree
  into `go.mod`. `goose` is used as a library by `api migrate`, so it needs no CLI. The Go
  toolchain is pinned to `go 1.27.4` (image `golang:1.27.4`).

## R2. Process topology

- **Decision**: two binaries.
  - `cmd/api`: the HTTP API, the WebSocket hub, and a `migrate` subcommand.
  - `cmd/worker`: the outbox relay/scheduler, the per-channel consumers, the reaper, the
    queue-depth sampler, and an ops HTTP server for `/metrics`, `/healthz` and `/readyz`.
  - The mock provider is a test/demo tool in `tools/mockprovider`, not a product service.
- **Rationale**: matches the constitution's api/worker split. The API never talks to RabbitMQ, so
  bursts of submissions can't be slowed down by the broker.
- **Alternatives**: a separate relay binary (one more deployable for no benefit at this scale); a
  single binary with flags (blurs scaling and failure isolation).

## R3. Message broker **(ADR: rabbitmq-over-alternatives)**

- **Decision**: RabbitMQ 3.13 (compatible with 4.x). Use **classic durable queues**, one per channel
  (`notifications.sms|email|push`), each declared with `x-max-priority=3`. Messages are persistent,
  published to a direct exchange `notifications` with routing key = channel. The dead-letter
  exchange `notifications.dlx` routes to `notifications.dlq`.
- **Rationale**:
  - Native per-message priority covers FR-017.
  - Per-channel queues give isolation between channels (US3-AS2, SC-009).
  - Ack and redelivery semantics plus dead-lettering are built in.
  - The broker has a management UI for demos.
  - Quorum queues aren't used: they only gained priorities in 4.0, and only two levels.
- **Alternatives**:
  - **Kafka**: no per-message priority; partition-ordered consumption makes per-item retry delays
    awkward; heavy to operate locally.
  - **Postgres-only queue** (`SKIP LOCKED` workers pulling straight from the table): simplest and
    viable at 300/s, but it couples delivery concurrency to DB connections and doesn't demonstrate
    an event-driven design, which is what the assessment asks for. It's recorded as the main
    "simpler alternative" in the ADR.

## R4. Transactional outbox and relay **(ADR: transactional-outbox)**

- **Decision**: the `notifications` table *is* the outbox; there's no separate outbox table. Rows
  that are due have `status IN ('pending','scheduled','retrying') AND next_attempt_at <= now()`.
  The relay ticks every 200 ms (configurable) and, in **one DB transaction**:
  1. `SELECT … FOR UPDATE SKIP LOCKED ORDER BY priority DESC, next_attempt_at LIMIT 500`
  2. guarded `UPDATE … SET status='queued' WHERE id = ANY($ids) AND status IN (…)`
  3. publishes each message with publisher confirms (`mandatory=true`) and waits for every confirm
  4. `COMMIT` (or `ROLLBACK` if any publish is nacked, returned or times out)
- **Rationale**:
  - The API only writes to the DB, so there are no dual writes (Constitution III).
  - `SKIP LOCKED` makes several worker replicas safe.
  - Publishing *inside* the transaction closes a race the input flow had: a consumer could receive
    a message while the row was still `pending`, skip it, and the row would then be committed as
    `queued` with no message left. The consumer claims in two steps in one transaction: first
    `SELECT … WHERE id=$1 FOR UPDATE` (no status predicate), then the guarded
    `UPDATE … WHERE status='queued'`. The locking read waits for the relay's row lock and returns
    the newest committed version, so the consumer sees either `queued` or, after a rollback,
    `pending` (drop; the relay publishes it again).
  - **Correction (found by the T043 integration tests)**: an earlier version of this entry said the
    guarded UPDATE alone would wait and re-check (EvalPlanQual). It doesn't. Under READ COMMITTED
    an UPDATE picks its candidate rows from its snapshot, where the row is still `pending`, so it
    matched 0 rows without waiting, and the message was dropped whenever it overtook the commit.
    The locking read is what makes the wait happen; `TestRepoClaimWaitsForAnOpenRelayTransaction`
    guards it.
  - A crash between publish and commit leaves the row due again, so it gets republished. The
    duplicate message is harmless because of the consumer guard (R7).
- **Cost**: row locks are held for one publish+confirm round-trip per tick (a few milliseconds for
  500 messages). This is acceptable and documented.
- **Backpressure**: the relay publishes a channel's due rows only while that channel has fewer
  than `RELAY_MAX_INFLIGHT_PER_CHANNEL` (default 1000) rows in `queued`. The rest of the backlog
  stays in Postgres as `pending`/`scheduled`/`retrying`. Each tick, for each channel, in the same
  transaction:
  1. `pg_try_advisory_xact_lock(hashtext('relay:' || channel))`. If another relay holds the lock,
     this relay skips the channel for this tick. The lock makes the cap exact when several worker
     replicas run, and it's released at commit.
  2. `inflight = count(*) WHERE channel=$c AND status='queued'`, which uses the in-flight partial
     index.
  3. `quota = min(RELAY_BATCH_SIZE, RELAY_MAX_INFLIGHT_PER_CHANNEL − inflight)`. If `quota ≤ 0`,
     skip the channel and increment `relay_backpressure_skips_total{channel}`.
  4. Pick `LIMIT quota` due rows for that channel, ordered `priority DESC, next_attempt_at`, then
     mark them `queued`, publish and confirm as above.
- **Why backpressure**: without it, a burst of 100k SMS would all go into RabbitMQ and drain at
  100/s, which takes about 17 min. Messages that are legitimately waiting would then be older than
  the reaper's 15-min `queued` threshold, and the reaper would republish thousands of them as
  duplicates. With the cap, a message's time in `queued` is about `cap / rate` = 10 s, far under
  the threshold. Priority still holds: the DB pick is `priority DESC`, and the RabbitMQ priority
  queue reorders whatever is already queued. Memory use and redelivery storms in the broker also
  stay bounded.
- **Worst case**: if the consumers stall (for example the limiter fails closed because Redis is
  down) for longer than 15 min, the reaper can republish at most `cap` messages per channel. The
  consumer guard drops these duplicates.
- **Alternatives**:
  - Mark `queued`, commit, then publish: a crash after commit leaves the row stuck in `queued`
    with no message, so the reaper would be needed on the happy path.
  - A separate outbox table: an extra write and extra cleanup, with no benefit when one row maps
    to one message.
  - Postgres `LISTEN/NOTIFY` instead of polling: adds latency jitter and complexity; 200 ms
    polling meets FR-018.

## R5. State machine **(spec override)**

- **Decision**: implement exactly FR-012 in `internal/domain`:

  ```
  pending   → queued | cancelled
  scheduled → queued | cancelled
  queued    → sending | cancelled
  sending   → sent | retrying | failed
  retrying  → queued | cancelled
  sent, failed, cancelled: terminal
  ```

  Every transition is persisted as `UPDATE … SET status=$to WHERE id=$id AND status = ANY($from)`
  and the row count is checked; 0 rows maps to `domain.ErrInvalidTransition` or "skip".
- **Spec overrides to the input flow**:
  - **Breaker open** (input: "status retrying"): the spec doesn't allow `queued → retrying`. The
    consumer moves `queued → sending`, records no attempt, then moves `sending → retrying`,
    without incrementing `attempts`, with `next_attempt_at = now + remaining cool-down + jitter`.
    Both steps run in one DB transaction. No rate-limit token is used (the breaker is checked
    before the token is acquired). If `now − max(created_at, scheduled_at)` exceeds
    `BREAKER_MAX_DEFER_AGE` (default 1 h), the second step is `sending → failed` instead, with
    `last_error='provider_unavailable'`, and the message is dead-lettered. This caps the deferral
    loop (Constitution III).
  - **Reaper** (input: "queued/sending → retrying"): `queued → retrying` isn't allowed. The reaper
    does two things:
    - `sending` older than `REAPER_SENDING_TIMEOUT` (default 2 min) → `retrying`, counting the
      abandoned call as an attempt (it may have reached the provider), or `failed` if attempts are
      used up.
    - `queued` older than `REAPER_QUEUED_TIMEOUT` (default 15 min) is **republished** without a
      status change. Relay backpressure (R4) keeps the normal time in `queued` around 10 s, so
      this happens only when a message was lost or the consumers stalled. The number republished
      is capped at `RELAY_MAX_INFLIGHT_PER_CHANNEL` per channel, and any duplicate is dropped by
      the consumer guard.

## R6. Delivery pipeline in the consumer

- **Decision**: for each delivery, in order:
  1. Decode the message. A poison message is `Nack(requeue=false)` and goes to the DLQ.
  2. If the channel's breaker is open, take the deferral path from R5, then ack.
  3. Acquire a rate-limit permit (R8), waiting with ctx; Redis errors are retried with backoff,
     failing closed. The total wait is capped by `LIMITER_MAX_WAIT` (default 30 s), whether the
     time goes to throttling or to Redis errors. When the cap is hit, the message is
     `Nack(requeue=true)` while the row is still `queued`, so it goes back to the broker and isn't
     lost; `rate_limiter_timeouts_total` is incremented. This caps the retry loop
     (Constitution III).
  4. Guarded `queued → sending`. If 0 rows (cancelled, duplicate, already done), ack and stop.
     This is the atomic cancel check required by FR-014.
  5. Call the provider through the channel's breaker with a 5 s timeout (FR-023).
  6. In one transaction: insert the `notification_attempts` row and apply the outcome
     (`sent` / `retrying` / `failed`).
  7. Ack after commit. For `failed`: commit, then `Nack(requeue=false)` so the DLX copies the
     message into `notifications.dlq` for inspection.
- **Rationale**: acquiring the permit *before* `sending` keeps the notification cancellable while it
  waits for throttling, which is where most of the time goes during a burst. Wasting a permit when
  step 4 skips is harmless.
- **Concurrency**: `CONSUMER_CONCURRENCY_PER_CHANNEL` (default 20) goroutines per channel, with
  AMQP `Qos(prefetch = concurrency)`. 20 in flight × ≥100 ms provider latency easily sustains
  100/s.

## R7. Idempotency and effectively-once **(ADR: at-least-once-idempotency)**

- **Decision**:
  - `idempotency_key` is `UNIQUE` (nullable).
  - `payload_hash` = SHA-256 of a canonical JSON of `{channel, recipient, content | template_id +
    sorted variables, priority, scheduled_at}`.
  - Insert with `ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`. If no row comes back,
    select the existing row: same hash → return it (202 + `Idempotent-Replayed: true` header);
    different hash → 409 `idempotency_conflict`.
  - The key can come from the `Idempotency-Key` header (single create) or the body field
    `idempotency_key` (single and per batch item). If both are present and differ, that's a 400.
  - Duplicate keys inside one batch: later items resolve to the first item's notification.
- **Downstream protections**:
  - The consumer guard (`WHERE status='queued'`) stops redelivered or duplicate messages from
    causing a second provider call for anything that already left `queued`.
  - The notification id is sent to the provider as an `Idempotency-Key` header, so a provider that
    deduplicates can also close the crash window.
- **Documented residual window**: the provider accepted the message but the worker died before
  committing `sent`. The reaper then retries it, which can deliver it twice. Exactly-once is not
  claimed.
- **Alternatives**: a Redis idempotency cache (not durable, which the constitution forbids for
  truth); idempotency at the HTTP layer only (doesn't cover batch items).

## R8. Distributed rate limiter **(ADR: redis-rate-limiter)** **(spec override)**

- **Decision**: **GCRA** (Generic Cell Rate Algorithm) in a single Redis Lua script, keyed by
  `ratelimit:{channel}`:
  - emission interval T = 1 s / `RATE_LIMIT_PER_SECOND` (default 100, so 10 ms)
  - burst tolerance 0
  - uses Redis `TIME`, so the clock is shared and worker clock skew doesn't matter
  - the script returns either "allowed" or "wait N µs", and the caller sleeps and retries
- **Rationale**: SC-003 requires "never more than 100 in **any** one-second window". The input's
  token bucket with capacity 100 lets up to ~200 through in a sliding second (a full bucket plus
  refill), which breaks the spec. GCRA with no burst spaces calls ≥ T apart across all workers,
  so any 1 s window holds at most 100. It's still "a token bucket of capacity 1", one atomic
  round-trip per call.
- **Failure mode**: if Redis is unreachable, the limiter **fails closed** (no sends, retry with
  backoff) and the worker becomes not-ready. This protects the provider. Each consumer gives up
  after `LIMITER_MAX_WAIT` and requeues the message (R6), so no goroutine holds a message
  indefinitely. Accepted messages wait in `queued` and are not lost.
- **Alternatives**: a sliding-window log (more memory, more Redis work); a token bucket with a small
  burst (still exceeds the strict window); dividing the limit statically per worker (breaks when
  replica count changes).
- **Amendment (implementation of T068/T069, measured on the compose stack)**:
  - **Slot reservation**: the script *reserves* the next slot (TAT += T) and returns how long to
    wait, so the caller sleeps until its own slot. That's one round trip per permit, with no
    thundering herd of retries. A slot further away than the caller's deadline isn't reserved.
  - **Permits ≠ provider calls**: slots were exactly T apart, but on the real stack a sliding
    second *at the provider* peaked at **106**. The permit comes before the claim (R6, so throttled
    items stay cancellable), and the claim's latency varies by tens of ms, so calls bunched. Fix:
    `Acquire` returns the slot time and the deliverer starts the provider call at
    `slot + RATE_LIMIT_SEND_OFFSET` (default 50 ms). If the claim overran the offset, it reserves a
    fresh slot instead of sending early; if none comes within the maximum wait, the claimed row
    goes back to `retrying` without an attempt. Peak dropped to 101–103.
  - **Connection pool**: the provider client keeps 256 idle connections per host. The default
    of 2 made most calls dial, adding variable latency between slot and arrival.
  - **Headroom** (`RATE_LIMIT_HEADROOM`, default 0.05 here; raised to 0.08 on 2026-11-19, see
    ADR 0004 "Headroom raised to 8%"): slots are spaced `T × 1.05`, about
    95/s at a limit of 100. Even then, millisecond CPU and network jitter on a loaded host left 1–3
    extra arrivals per second, because a 10 ms slot leaves no room for jitter. With 5% headroom it
    would take about 50 ms of jitter to exceed the limit. Measured peak: **96–98**. Sustained rate
    stays inside SC-003's 90–100 band. `0` restores exact spacing.
  - **Fail-open race (bug, fixed)**: right after a deadline passes, `ctx.Err()` can still be nil
    for a moment. `Acquire` returned `(zero, nil)` there, so a notification could be sent
    *without a permit* while Redis was down (seen 1 in 10 runs of the outage test). The limiter now
    returns `DeadlineExceeded` explicitly, and the deliverer rejects a zero slot (defence in
    depth). A deterministic unit test models the race.

## R9. Retry, backoff and circuit breaker **(ADR: retry-and-circuit-breaker)**

- **Classification** (spec FR-020, applied to any 2xx/non-2xx):
  - **2xx → success.** The input said "202 only", but webhook.site returns 200 by default, and the
    spec only says "accepted response".
  - 5xx, 429, timeout, connection/DNS error → transient.
  - Every other status (other 4xx, plus unexpected 1xx/3xx with redirects disabled) → permanent.
  - A 2xx with an unparsable body → sent, `provider_message_id = NULL`, warning logged.
- **Backoff**: `delay = rand(0, min(5 min, 1 s × 2^(attempt-1)))` (full jitter), then
  `max(delay, Retry-After)` when the provider sends Retry-After (seconds or HTTP-date).
  `attempts` counts provider calls. On a transient failure, if `attempts >= max_attempts`
  (default 5, stored per row) the notification becomes `failed` and goes to the DLQ.
- **Breaker**: one `sony/gobreaker/v2` breaker per channel per worker process.
  - Trips after `BREAKER_CONSECUTIVE_FAILURES=5` consecutive transient failures.
  - `Timeout=30s`, `MaxRequests=1` while half-open.
  - `IsSuccessful` returns true for success *and* for permanent 4xx, so a bad recipient doesn't
    trip the channel.
  - `ErrOpenState` and `ErrTooManyRequests` both take the deferral path (R5), which doesn't use an
    attempt (FR-024).
  - **Deferral cap**: a deferral for a notification whose age, measured from
    `max(created_at, scheduled_at)`, exceeds `BREAKER_MAX_DEFER_AGE` (default 1 h) becomes
    `failed` with `last_error='provider_unavailable'` and goes to the DLQ
    (`notifications_dead_lettered_total{reason="provider_unavailable"}`). Without it, a provider
    that never recovers would leave notifications non-terminal forever.
- **Trade-off**: the breaker state is per process, not shared. With N workers, up to N trial calls
  can happen per cool-down. That's acceptable ("stops hammering", not "exactly one probe") and
  avoids yet another Redis round-trip per call. Documented in the ADR.

## R10. Priority semantics

- **Decision**: store `priority` as a smallint equal to the AMQP priority: `high=3`, `normal=2`,
  `low=1`. The relay sorts `priority DESC, next_attempt_at ASC`, and the queue itself is a priority
  queue.
- **Known bound**: messages already prefetched by consumers, which can be at most
  `concurrency × workers` per channel, have been taken off the queue. A newly arrived high-priority
  item can therefore be behind up to that many normal/low items. That's consistent with the spec's
  "a delivery already in progress is never pre-empted", and the README states the bound. The
  SC-007 test asserts the high item lands within the first `concurrency × workers + relay batch`
  sends.

## R11. Real-time updates **(ADR: realtime-fanout-via-redis-pubsub)**

- **Decision**: after each status-changing commit, the app layer calls the `StatusBroadcaster`
  port. The Redis adapter does `PUBLISH notification-status <json>`.
  - The API decides whether to publish for create/cancel; the worker publishes for
    queued/sending/sent/retrying/failed.
  - Each API instance holds one subscription and fans out to local `coder/websocket` clients,
    indexed by notification id and batch id.
  - When a client subscribes, the current status is sent first as a snapshot, then live changes.
  - Unknown ids are rejected with 404 *before* the upgrade.
  - Slow clients get a bounded send buffer (64) and are dropped with close code 1008 if it fills.
- **Rationale**: this meets FR-030 across processes, and Redis stays non-durable as the
  constitution requires. Delivery to subscribers is **best-effort** (a missed publish isn't
  replayed). The snapshot on connect and the status endpoint are the way to recover, which matches
  the spec's "no replay required" assumption.
- **Alternatives**: Postgres `LISTEN/NOTIFY` (works, but holds a dedicated connection per API
  instance and has payload limits); a RabbitMQ fanout exchange (would make the API depend on the
  broker).

## R12. Templates **(spec override)** **(ADR: restricted-templates)**

- **Decision**:
  - Templates are channel-agnostic, per the spec's assumption, so the `channel` column from the
    input is dropped. Fields are `id`, unique `name`, `body`, `variables` (derived), `created_at`.
  - Creation parses the body with `text/template` and walks the parse tree. **Only** plain field
    actions `{{.Var}}` and text are allowed; `range`/`if`/`with`/pipelines/function calls/nested
    templates are rejected with 400. The derived variable list is stored.
  - Rendering happens at submission time: missing variables (`required − provided`) are all named
    in one 400; extra variables are ignored. Output is produced with `missingkey=error` as a
    safety net, then validated against the channel limits.
  - A body is at most 100,000 characters.
- **Rationale**: restricting templates to plain fields lets the system name *every* missing
  variable, which `text/template` alone can't do because it stops at the first error. It also
  removes template-injection and runaway-output risks. Rendering on the API side snapshots the
  content, so later template changes can't alter what's already queued.
- **Alternatives**: the full `text/template` feature set (unsafe and unbounded for untrusted
  input); rendering in the worker (validation errors would surface asynchronously, which breaks
  US8-AS2).

## R13. Persistence access and pagination **(ADR: keyset-pagination)**

- **Decision**:
  - `pgx/v5` pool with `sqlc`-generated queries in `sql/queries/`.
  - Dynamic list filters use `sqlc.narg` with `IS NULL OR` predicates.
  - Batch insert is a single `INSERT … SELECT FROM unnest(...) ON CONFLICT DO NOTHING RETURNING`,
    one round-trip for 1,000 rows (SC-002).
  - Ids are UUIDv7 (`google/uuid`), so they sort by time.
  - The list cursor is base64url of `{created_at, id}`, ordered `created_at DESC, id DESC`, with
    predicate `(created_at, id) < ($c, $i)`. A malformed cursor is a 400.
  - Page size defaults to 20; above 100 is a 400 (spec).
- **Rationale**: keyset pagination is stable under concurrent inserts (no duplicates or gaps,
  US6-AS1) and costs the same for page 1 and page 1,000.
- **Alternatives**: offset pagination (drifts under writes, slows down on deep pages); GORM
  (hides SQL, and guarded updates must be explicit).

## R14. Migrations

- **Decision**: `goose` SQL migrations in `migrations/`, embedded through `embed.FS`, run by
  `api migrate up`. In docker-compose a one-shot `migrate` service runs first, and `api` and
  `worker` use `depends_on: condition: service_completed_successfully`. `make migrate` runs the
  same command.
- **Rationale**: if both binaries ran migrations on startup they would race. A single step that
  runs before either starts makes the order deterministic. This is documented as a design note in
  the README rather than an ADR.

## R15. HTTP layer and contract **(spec override for non-JSON endpoints)**

- **Decision**:
  - `api/openapi.yaml` (OpenAPI 3.0.3) is the canonical contract. During design it's drafted in
    [contracts/openapi.yaml](./contracts/openapi.yaml), and the first implementation task moves it
    to `api/openapi.yaml`, after which that path is the only copy.
  - `oapi-codegen` generates chi + strict-server code into `internal/adapters/http/gen`.
  - The `realtime` and `ops-raw` tags are excluded from codegen (`/ws` needs connection hijacking;
    `/metrics` is Prometheus text; `/swagger` is static). They're still documented in the YAML and
    mounted by hand on the same chi router. This is recorded in the plan's Complexity Tracking
    table.
  - `/swagger` serves a small embedded HTML page that loads Swagger UI from a pinned CDN version,
    pointed at `/openapi.yaml`.
  - Middleware order: recover → correlation id → request logging → metrics → timeout.
  - Server timeouts: `ReadHeaderTimeout` 5 s, `ReadTimeout` 15 s, `WriteTimeout` 30 s; body limit
    of 10 MiB for a batch of 1,000 × 100 KB emails.
  - Validation happens in the domain (channel rules) and the app layer (batch/template). Results
    map to `{"error":{"code","message","details":[{"field","message"}]}}`.

## R16. Observability **(spec override for queue depth)**

- **Decision**:
  - **Logs**: slog JSON. A `correlation_id` comes from the `X-Correlation-ID` header or is
    generated (UUIDv7), and is echoed in the response. It's stored on the notification and batch,
    carried in an AMQP header, and put in the worker's log context for every message. A helper
    `logger.From(ctx)` makes sure no log line on the request or message path is missing it.
  - **Metrics**: the list in [contracts/observability.md](./contracts/observability.md). The
    input's `queue_depth{channel}` came from a RabbitMQ passive declare, which can't split by
    priority, but FR-031 requires queue depth **per channel and priority**. The worker's sampler
    (every 5 s) therefore sets `notification_backlog{channel,priority,status}` from a `GROUP BY` on
    the partial index of non-terminal rows. The broker-side `rabbitmq_queue_messages{queue}` gauge
    is kept as a second signal.
  - All latencies are histograms.
  - The API and the worker each expose `/metrics`, `/healthz` and `/readyz`.
  - `/api/v1/metrics/summary` is a JSON view computed from the DB (backlog by
    channel/priority/status; sent/failed/retried over the last 5 min; p50/p95/p99 e2e and provider
    latency over the last 5 min via `percentile_cont`). Built after the required features
    (Constitution VII).
- **Readiness**:
  - API: Postgres is required. Redis being down is reported as `degraded` but the API stays ready,
    because only WebSocket fan-out depends on Redis and accepting notifications must not.
  - Worker: Postgres, RabbitMQ and Redis are all required.
  - Liveness on both checks the process only.
- **Tracing (bonus)**: OpenTelemetry OTLP → Jaeger. W3C `traceparent` goes in AMQP headers. It's
  off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## R17. Graceful shutdown

- **Decision**: a root context is cancelled by SIGTERM/SIGINT. Then, in order:
  - API: stop accepting connections, `http.Server.Shutdown` with `SHUTDOWN_TIMEOUT` (default 20 s),
    close WebSocket clients with 1001, close Redis and then the DB.
  - Worker: stop the relay and reaper tickers; cancel the AMQP consumers so no new deliveries
    arrive; wait for in-flight handlers (bounded by provider timeout + DB commit); unacked messages
    return to the queue; close AMQP, Redis, then the DB.
  - `errgroup` coordinates the steps.

## R18. Testing approach

- **Decision**:
  - Unit tests are table-driven with stdlib `testing` and `testify/require`. They use fakes for
    the ports (in-memory repo, fake clock, fake provider, fake limiter) and cover the state
    machine, validation, backoff, classification, template parsing/rendering and the delivery use
    case.
  - Integration tests use the `integration` build tag and `testcontainers-go` modules for
    postgres, rabbitmq and redis, plus an `httptest` mock provider driven per test.
  - `make test` runs `go test -race ./...` with `-tags integration`. `make test-unit` skips the
    tag.
  - CI uses GitHub Actions jobs: lint → unit → integration (Docker available on ubuntu runners)
    → build images. CI is set up in phase A, so every later commit is gated by it.
- **Timing-sensitive tests**: the rate-limit test uses limit 20/s for 3 s and checks a sliding
  1-second window over the mock provider's timestamps. Scheduled and backoff tests use short
  configured durations, not real minutes.

## R19. Configuration (defaults → `.env.example`)

| Variable | Default | Used by |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://notify:notify@postgres:5432/notify?sslmode=disable` | api, worker |
| `AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/` | worker |
| `REDIS_ADDR` | `redis:6379` | api, worker |
| `HTTP_ADDR` / `OPS_ADDR` | `:8080` / `:9090` | api / worker |
| `PROVIDER_URL` | `http://mockprovider:8090/send` | worker |
| `PROVIDER_TIMEOUT` | `5s` | worker |
| `RATE_LIMIT_PER_SECOND` | `100` | worker |
| `RATE_LIMIT_SEND_OFFSET` | `50ms` | worker |
| `RATE_LIMIT_HEADROOM` | `0.08` (was 0.05; ADR 0004) | worker |
| `MAX_ATTEMPTS` | `5` | api (stamped per row) |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `5m` | worker |
| `BREAKER_CONSECUTIVE_FAILURES` / `BREAKER_COOLDOWN` | `5` / `30s` | worker |
| `BREAKER_MAX_DEFER_AGE` | `1h` | worker |
| `LIMITER_MAX_WAIT` | `30s` | worker |
| `RELAY_INTERVAL` / `RELAY_BATCH_SIZE` | `200ms` / `500` | worker |
| `RELAY_MAX_INFLIGHT_PER_CHANNEL` | `1000` | worker |
| `CONSUMER_CONCURRENCY_PER_CHANNEL` | `20` | worker |
| `REAPER_INTERVAL` / `REAPER_SENDING_TIMEOUT` / `REAPER_QUEUED_TIMEOUT` | `30s` / `2m` / `15m` | worker |
| `SHUTDOWN_TIMEOUT` | `20s` | api, worker |
| `LOG_LEVEL` | `info` | all |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(empty = tracing off)* | api, worker |

## R20. Dependencies (justification per Constitution VI)

| Dependency | Removes complexity of |
|------------|-----------------------|
| `go-chi/chi/v5` | routing + middleware composition (the target of oapi-codegen's chi output) |
| `oapi-codegen` runtime | contract-generated request/response types and validation |
| `jackc/pgx/v5`, `sqlc` | Postgres driver/pool; type-safe SQL without an ORM |
| `pressly/goose/v3` | versioned migrations |
| `rabbitmq/amqp091-go` | the AMQP 0-9-1 protocol (no stdlib equivalent) |
| `redis/go-redis/v9` | the Redis protocol, Lua scripts, pub/sub |
| `sony/gobreaker/v2` | a correct circuit-breaker state machine |
| `coder/websocket` | the WebSocket protocol (context-aware, maintained) |
| `prometheus/client_golang` | exposition format and histograms |
| `google/uuid` | UUIDv7 |
| `stretchr/testify`, `testcontainers-go` | assertions; disposable real dependencies |
| `go.opentelemetry.io/otel` *(bonus)* | trace propagation/export |

No NEEDS CLARIFICATION items remain.
