# ADR 0005: Retry with full-jitter backoff, per-channel circuit breakers and a deferral cap

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R9 (and R5 for the deferral path); analysis finding D1

## Context

The external provider fails in three ways:
- for one message, e.g. a bad recipient (4xx);
- briefly, e.g. a 5xx, 429, timeout or connection reset;
- for minutes at a time, an outage.

The system must retry transient failures with exponential backoff, give up after a bounded number
of attempts (FR-020–FR-023), and stop hammering a provider that is down. It must do that without
burning the retries of every queued notification during the outage, and without pausing healthy
channels (FR-024, SC-009). A notification also can't stay non-terminal forever if the provider
never comes back (finding D1).

## Decision

**Classification** (`domain.Classify`):

| Response | Outcome |
|---|---|
| any 2xx | success (a body without a parsable message id is still sent, with a warning) |
| 5xx, 429, timeout, connection or DNS error | transient |
| any other status (other 4xx, unexpected 1xx/3xx; redirects are disabled) | permanent |

**Retry and backoff**:
- `attempts` counts provider calls, and `max_attempts` (default 5) is stamped on each row when it's
  created.
- A transient failure with attempts left goes `sending → retrying` with
  `next_attempt_at = now + max(rand(0, min(BACKOFF_MAX, BACKOFF_BASE × 2^(attempt−1))), Retry-After)`.
  That's full jitter, with defaults of 1 s base and a 5 min cap. Retry-After is accepted as
  seconds or as an HTTP date.
- The outbox relay re-publishes the row when it's due. Delays live in Postgres, not in broker TTL
  queues (ADR 0002).
- A permanent failure, or a transient one on the last attempt, goes to `failed`. The consumer then
  rejects the message without requeue, so the DLX routes it to `notifications.dlq`, and the
  reason is counted as `permanent` or `exhausted`.

**Circuit breaker**:
- There's one `sony/gobreaker/v2` breaker per channel per worker process.
- A breaker trips after `BREAKER_CONSECUTIVE_FAILURES` (default 5) consecutive transient failures.
  It stays open for `BREAKER_COOLDOWN` (30 s), then lets exactly one trial call through
  (half-open).
- Permanent failures count as successes for the breaker: a bad recipient isn't an outage.
- State changes feed `circuit_breaker_state{channel}` (0 closed, 1 half-open, 2 open).

**Deferral while open**:
- While its channel's breaker is open (or half-open and busy), a claimed message isn't sent. The
  row goes `sending → retrying` with `attempts` unchanged and no attempt row (FR-024).
- `next_attempt_at` is the remaining cool-down plus up to 25% jitter, so recovering workers don't
  all probe at once.
- The worker checks the breaker before taking a rate-limit permit, so deferrals don't consume the
  shared 100/s budget.

**Deferral cap (D1)**:
- If a deferred notification's age exceeds `BREAKER_MAX_DEFER_AGE` (default 1 h), it goes to
  `failed` with `last_error = provider_unavailable` and lands in the DLQ
  (`notifications_dead_lettered_total{reason="provider_unavailable"}`) through the same DLX path.
  Age is measured from
  `max(created_at, scheduled_at)`, so a scheduled notification isn't penalised for waiting until
  its time.
- When a deferral is rescheduled, `next_attempt_at` is clamped to the cap moment. A notification
  therefore fails at the cap rather than up to one cool-down later.

## Consequences

- **Per-process breaker state (trade-off).** Breakers aren't shared across workers. With N
  workers:
  - Up to N trial calls can reach a failing provider per cool-down, instead of one.
  - Each worker needs its own 5 failures to trip, so up to 5 × N failed calls can happen before
    every worker stops.
  - A shared Redis breaker would make this exactly one, but it adds a Redis round-trip to every
    provider call and a second failure mode in the hot path. The goal is "stop hammering", not
    "exactly one probe", so N small bursts are acceptable.
  - If the fleet grows large, the next step is a shared "open until" key that workers read on
    deferral. They would still decide locally.
- An outage costs no attempts, so notifications don't exhaust their retries while the provider is
  down. Once it recovers, they're delivered within about one cool-down, which the breaker
  integration test verifies.
- The deferral cap bounds how long a notification can stay non-terminal, which keeps the backlog
  and the "pending forever" question finite. The cost is that a provider outage longer than 1 h
  dead-letters traffic, which can then be replayed from the DLQ. Operators who prefer to wait
  longer can raise `BREAKER_MAX_DEFER_AGE`.
- Full jitter spreads retries after a mass failure. An exact retry time isn't predictable, but
  `next_attempt_at` is stored and visible in `GET /api/v1/notifications/{id}`.

## Alternatives considered

- **Count deferrals as attempts.** This is simpler, but a 3-minute outage would exhaust every
  queued notification's retries and dead-letter them, which violates FR-024.
- **Broker TTL/delay queues for retries.** This duplicates the scheduling that the outbox already
  does, splits the source of truth between Postgres and RabbitMQ, and makes `next_attempt_at`
  unobservable.
- **A shared breaker in Redis.** See the per-process trade-off above.
- **No deferral cap.** A provider that never recovers would leave notifications `retrying`
  forever (finding D1).
