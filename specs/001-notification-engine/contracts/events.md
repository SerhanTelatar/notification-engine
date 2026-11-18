# Contract: Messages and Events

The interfaces between processes that aren't HTTP. A change to any of these is a contract change.

## 1. Work messages (RabbitMQ)

**Topology** (the worker declares it on startup; every declaration is idempotent)

| Kind | Name | Properties |
|------|------|------------|
| exchange | `notifications` | `direct`, durable |
| queue | `notifications.sms`, `notifications.email`, `notifications.push` | durable, classic, `x-max-priority=3`, `x-dead-letter-exchange=notifications.dlx` |
| binding | `notifications` → `notifications.{channel}` | routing key = channel |
| exchange | `notifications.dlx` | `fanout`, durable |
| queue | `notifications.dlq` | durable, no consumer (for inspection) |

**Publish properties**

- `delivery_mode=2` (persistent)
- `priority` ∈ {1,2,3}, equal to the domain priority
- `content_type=application/json`
- `message_id` = notification id
- `mandatory=true`
- The publisher waits for a confirm on every message.

**Headers**

- `x-correlation-id`
- `traceparent` (only when tracing is on)

**Body** (deliberately minimal; the DB row is authoritative, so content isn't copied into the
message)

```json
{ "notification_id": "0193…", "channel": "sms", "priority": 3, "enqueued_at": "2026-11-18T10:00:00Z" }
```

**Consumer rules**

- Messages are idempotent by design. The consumer only acts if its guarded
  `UPDATE … WHERE status='queued'` matches a row. Otherwise it acks and drops the message.
- It acks only after the outcome transaction commits.
- On a terminal `failed`, it commits and then calls `Nack(requeue=false)`, so the message is
  dead-lettered to `notifications.dlq`.
- An undecodable message is sent straight to `Nack(requeue=false)`.

## 2. Status events (Redis pub/sub)

- **Channel**: `notification-status`
- **Producer**: whichever process committed the transition (api for create/cancel, worker for
  every other transition).
- **Delivery**: best-effort, after commit. Losing an event never affects correctness.

```json
{
  "notification_id": "0193…",
  "batch_id": "0193…",
  "from": "queued",
  "to": "sending",
  "attempts": 1,
  "at": "2026-11-18T10:00:00.123Z",
  "correlation_id": "c-123"
}
```

`batch_id` is `null` for single notifications. `from` is `null` on create.

## 3. WebSocket protocol (`GET /ws`)

- Query: exactly one of `notification_id` or `batch_id` (see `/ws` in openapi.yaml). A missing,
  duplicated or malformed id returns 400 before the upgrade; an unknown id returns 404.
- The server only sends; anything the client sends is ignored except ping/pong and close.
- On connect the server sends one `snapshot` message, then a `status` message per change:

```json
{ "type": "snapshot",
  "notifications": [ { "notification_id": "…", "status": "queued", "updated_at": "…" } ] }
```

```json
{ "type": "status", "notification_id": "…", "batch_id": "…", "from": "sending", "to": "sent",
  "attempts": 1, "at": "…" }
```

- Keepalive: the server pings every 30 s and closes the connection if no pong arrives within 10 s.
- Backpressure: each client has a send buffer of 64 messages. On overflow the server closes with
  `1008` (policy violation) and the client should reconnect, which gets it a fresh snapshot.
- Shutdown: the server closes with `1001` (going away).
- A notification subscription is **not** closed automatically when the notification reaches a
  terminal status. The client decides when to close.
