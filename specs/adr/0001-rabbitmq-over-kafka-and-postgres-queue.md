# ADR 0001: RabbitMQ for work dispatch (over Kafka or a Postgres-only queue)

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R3

## Context

Accepted notifications are delivered asynchronously over three channels, each limited to
100 deliveries/s. The transport must support:

- delivering high priority before normal before low (FR-017)
- keeping a backlog or failure on one channel from blocking the others (US3-AS2, SC-009)
- redelivering work after a worker crash (FR-025)
- dead-lettering failed work (FR-022)

PostgreSQL is the source of truth (Constitution III), so the transport only carries "work to do"
signals.

## Decision

- Use **RabbitMQ** with one durable **classic** queue per channel (`notifications.sms`,
  `notifications.email`, `notifications.push`), each declared with `x-max-priority=3`.
- Messages are persistent and published to a `direct` exchange `notifications` with the channel as
  routing key. Publishers use confirms.
- A dead-letter exchange `notifications.dlx` routes to `notifications.dlq`.
- Quorum queues are not used because they only support two priority levels (4.0+).
- The full topology is in `contracts/events.md`.

## Alternatives considered

- **Kafka**:
  - no per-message priority, so priority would need one topic per priority and
    consumer-side merging
  - consumption is ordered per partition, so delaying a single message for retry blocks its
    partition or needs retry topics
  - heavier to run locally
- **Postgres-only queue**: workers would claim rows with `FOR UPDATE SKIP LOCKED` and call the
  provider directly.
  - This is the simplest option and would handle 300/s.
  - It ties delivery concurrency to DB connections and holds a row lock for the whole provider
    call.
  - It doesn't show an event-driven design, which is part of what this assessment asks for.
  - It remains the fallback if the broker ever proves unnecessary.

## Consequences

- Priority, per-channel isolation, ack/redelivery and dead-lettering come built in, and the
  management UI helps demos.
- Messages carry only the notification id; the DB row is authoritative, so content isn't
  duplicated in the broker.
- A message can be lost or duplicated in a few edge cases. Both are handled outside the broker:
  - the outbox relay plus the reaper's republish covers a lost message (ADR 0002)
  - the consumer's guarded `queued → sending` update drops duplicates (ADR 0003)
- One more piece of infrastructure to run. docker-compose starts it with a healthcheck.
- Priority isn't strict for messages consumers have already prefetched, at most
  `concurrency × workers` per channel. SC-007 allows for this.
