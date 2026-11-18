# ADR 0007: Real-time status updates fanned out over Redis pub/sub to WebSocket clients

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R11 (US9, FR-030, SC-010)

## Context

Clients want to watch a notification, or a whole batch, change status live over a WebSocket
(US9). Status changes are committed by two kinds of process:
- the API commits create and cancel;
- the worker commits queued, sending, sent, retrying and failed.

The WebSocket clients are connected to *some* API instance, so every transition, whichever
process commits it, has to reach every API instance. The spec requires an update within 1 s
(SC-010) and doesn't require replaying missed events. Redis is already in the stack, but per the
constitution it must never be a source of truth.

## Decision

**Publish after commit.**
- After each status-changing commit, the use case calls the `StatusBroadcaster` port.
- The Redis adapter (`StatusBus`) runs `PUBLISH notification-status <json>` with the payload in
  `contracts/events.md` §2.
- The publish is detached from the caller's cancellation (the commit already happened), but it's
  bounded at 500 ms.
- A failure is logged and never fails or delays the use case.

**One subscription per API instance.**
- A `status-subscriber` component subscribes to the channel and hands each event to an
  in-process hub. go-redis reconnects the subscription after an outage.
- The hub indexes clients by notification id and by batch id, encodes each event once, and
  delivers without blocking.

**The `/ws` protocol** (`contracts/events.md` §3) works like this:
1. Exactly one well-formed `notification_id` or `batch_id` is required. An unknown id returns
   404, and every check happens **before** the upgrade.
2. The handler subscribes to the hub first and then reads the current status, so nothing
   committed in between is lost.
3. It sends one `snapshot` message, then a `status` message per change.
4. It pings every 30 s and closes if no pong arrives within 10 s. Client data frames are read and
   ignored.
5. Each client has a 64-message buffer. On overflow the hub drops that client with 1008, and the
   client should reconnect for a fresh snapshot. One slow client never stalls the others.
6. On shutdown, every client is closed with 1001.

**Best-effort by design.**
- A publish lost while Redis is down, or an event for a dropped client, is not replayed.
- The snapshot on (re)connect and `GET /api/v1/notifications/{id}` are how clients recover.
- Redis is therefore *optional* for API readiness (`degraded`).

## Consequences

- Updates arrive in tens of milliseconds. SC-010 (≤ 1 s from commit to subscriber) is asserted in
  `test/integration/ws_test.go`, with the worker and the API as separate components joined only by
  Redis.
- API instances scale horizontally with no affinity: each one receives every event and keeps the
  ones its own clients want. That's fine at this event volume, at most a few hundred per second
  per channel. Very large fleets would want sharded channels per batch or tenant.
- **Clients must tolerate gaps and duplicates.**
  - An event can be missed (during an outage or after an overflow), which is why the snapshot
    exists.
  - A change committed between subscribing and reading the snapshot may appear in both.
  - Events can arrive out of order, even from one process. The relay announces `queued` after
    its commit, and by then a consumer may already have claimed the row and announced `sending`.
    Across processes, a worker's `queued` can race an API-side `cancelled`. Publishes go out
    concurrently on pooled connections, so there's no ordering per notification. The `at` field,
    the `from`/`to` pair and the snapshot let clients reconcile; the latest `at` wins.
- No durable fan-out state exists anywhere. Restarting an API instance just drops its sockets
  (1001), and the clients reconnect.

## Alternatives considered

- **Postgres `LISTEN/NOTIFY`**: it's transactional, so it never announces an uncommitted change.
  But it ties up a dedicated connection per API instance, has an 8 KB payload limit, and puts
  fan-out load on the source-of-truth database.
- **A RabbitMQ fanout exchange**: it would make the API depend on the broker, which it otherwise
  doesn't. Durable queues per instance would also be more machinery than best-effort updates need.
- **Polling the status endpoint**: it's simple, but it can't meet 1 s cheaply across many
  watchers, and it's what the snapshot/GET fallback already provides.
- **Replaying missed events from an outbox table**: it would make updates exactly-once, which the
  spec doesn't ask for, at the cost of retention, cursors and much more API state.
