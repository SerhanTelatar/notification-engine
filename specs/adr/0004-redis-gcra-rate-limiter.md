# ADR 0004: Distributed rate limiting with GCRA in Redis, paced on the slot

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R8 (and its implementation amendment)

## Context

FR-016 and SC-003: at most 100 deliveries per second **per channel**, measured across **all
workers combined**, and never more than 100 in **any** one-second window. Under a sustained
backlog the rate must stay at 90–100/s. Workers scale horizontally, so no single process can
enforce the limit on its own, and Redis must never be a source of truth (Constitution III).

## Decision

- **GCRA with zero burst, as a slot reservation in one Lua script** on `ratelimit:{channel}`.
  - The key holds the time of the next free slot (TAT).
  - Each call reserves `max(TAT, now)` and advances TAT by `T = 1s / limit × (1 + headroom)`.
  - The script returns how long the caller must wait.
  - Redis `TIME` is the only clock, so worker clock skew can't break the limit.
  - It's one round trip per permit, and waiters don't re-poll.
- **Caller deadline**: a slot further away than the caller's deadline (`LIMITER_MAX_WAIT`) isn't
  reserved. The consumer then requeues the message, and the row stays `queued` and cancellable.
- **Permit before the claim, call paced on the slot.**
  - The permit is taken before the `queued → sending` claim, so throttled items stay
    cancellable (R6).
  - `Acquire` returns the slot time, and the provider call starts at
    `slot + RATE_LIMIT_SEND_OFFSET` (50 ms).
  - If the claim overran the offset, the deliverer reserves a fresh slot instead of sending
    early.
  - If no fresh slot comes in time, the claimed row returns to `retrying` without using an
    attempt.
- **Headroom** (`RATE_LIMIT_HEADROOM`, default 0.05): slots are spaced 5% wider than `1s/limit`,
  about 95/s at a limit of 100.
- **Fail closed**: a Redis error is never a permit. The deliverer retries with backoff up to
  `LIMITER_MAX_WAIT`, then requeues, and worker readiness requires Redis. A permit without a slot
  time is also rejected (defence in depth).

## Alternatives considered

- **Token bucket of capacity 100** (the planning input): a full bucket plus refill lets about 200
  through in a sliding second, which breaks SC-003.
- **Token bucket with a small burst**: still exceeds the strict window by the burst size.
- **Sliding-window log** (sorted set of timestamps): exact, but O(limit) memory and more Redis
  work per call, for no gain over GCRA.
- **Static split per worker** (limit / replicas): wrong as soon as replicas change or a worker is
  idle.
- **Permit after the claim, right before the call**: it would avoid the pacing offset, but
  throttled items would sit in `sending` and couldn't be cancelled.
- **Exact spacing without headroom**: see the measurements below.

## Measurements that shaped the decision

On `docker compose up` (8 CPUs shared by every service and the load generator):

| Version | Peak calls at the provider in a sliding 1 s window (limit 100) |
|---------|------------------------------------------------------------------|
| Permits exactly spaced, sent right after the claim | 106 |
| + calls paced on `slot + 50 ms` | 101–103 |
| + provider connection pool sized for the consumers | 101–103 |
| + 5% headroom | **96–98** |

- The permits were always exactly spaced; the excess came from latency between permit and
  arrival. At 100/s a slot is only 10 ms, so a few milliseconds of CPU or network jitter adds an
  extra arrival to a window. Headroom turns "about 10 ms of jitter breaks the limit" into "about
  50 ms of jitter does".
- In the integration test (20/s, two workers, no load generator), the provider-side peak is
  exactly the limit with headroom 0.

## Consequences

- The limit holds across any number of workers and against worker clock skew. Under sustained
  backlog the rate is about 95/s per channel (measured 95–96/s at a limit of 100).
- Every delivery gains `RATE_LIMIT_SEND_OFFSET` (50 ms) of latency, a small cost against SC-001
  (ingestion) and SC-008 (5 s for scheduled items).
- A reserved slot that its caller abandons is wasted: a little lost throughput, never an extra
  send.
- Losing Redis state at most allows a burst of one slot right after a restart. While Redis is
  down nothing is sent; messages wait in `queued` and resume when Redis returns (tested through a
  TCP proxy outage).
- **Environment note**: with single creates from 50 concurrent `curl` processes, delivery
  dropped to about 55–65/s *while* the requests were arriving and recovered to 95–96/s once they
  stopped. The load generator was competing for the same 8 CPUs. With the batch endpoint as the
  ingestion path (`scripts/demo-burst.sh`, 1,000 items per request), 3,000 SMS are accepted in
  under a second and delivered at a steady 95/s from start to finish, with a peak of 97 at the
  provider.
- A fail-open race (`ctx.Err()` still nil just after the deadline) was found by the outage
  integration test and fixed. It's recorded in the README's "Bugs caught by tests" backlog.
