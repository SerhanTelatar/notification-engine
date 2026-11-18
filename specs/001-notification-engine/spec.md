# Feature Specification: Event-Driven Notification Engine

**Feature Branch**: `001-notification-engine`

**Created**: 2026-11-18

**Status**: Draft

**Input**: User description: "Build an event-driven notification system that accepts notification
requests over an HTTP API and delivers them asynchronously through three channels — SMS, Email and
Push — via an external provider. It must handle burst traffic (flash sales, breaking news), retry
failed deliveries intelligently, and give internal teams and API consumers real-time visibility
into delivery status." (full description, user stories and acceptance criteria supplied with the
`/speckit-specify` command)

## User Scenarios & Testing *(mandatory)*

**Actors**

- **API consumer**: an internal service that submits notifications and tracks their outcome.
- **Operator**: an engineer monitoring system health, backlog and throughput.

### User Story 1 - Submit a notification and have it delivered reliably (Priority: P1)

An API consumer submits a single notification (recipient, channel, content, optional priority,
schedule time and idempotency key). The system validates it, acknowledges it immediately with an
id and status `pending`, and delivers it to the external provider in the background. Transient
provider failures are retried with growing, randomised delays; permanent failures stop immediately.
Resubmitting with the same idempotency key never produces a second delivery.

**Why this priority**: accepting and reliably delivering a notification is the reason the system
exists. Every other story builds on it.

**Independent Test**: submit a valid notification against a mock provider and confirm it reaches
the provider exactly once and ends in `sent`. Configure the mock to fail transiently and
permanently, and confirm retry and fail behaviour.

**Acceptance Scenarios**:

1. **Given** a valid SMS request, **When** the consumer submits it, **Then** the system responds
   with 202, a notification id and status `pending`, without waiting for the provider.
2. **Given** an accepted notification and a healthy provider, **When** processing runs, **Then**
   the provider receives it once and the notification becomes `sent` with the provider message id
   recorded.
3. **Given** an SMS request with a recipient that is not E.164 or content over 160 characters,
   **When** submitted, **Then** the system responds 400 with a field-level error for each invalid
   field and nothing is stored.
4. **Given** an email request with an invalid address or content over 100,000 characters, **When**
   submitted, **Then** the system responds 400 with field-level errors.
5. **Given** a push request with an empty device token or content over 4,000 bytes, **When**
   submitted, **Then** the system responds 400 with field-level errors.
6. **Given** a request with no content and no template, **When** submitted, **Then** the system
   responds 400 identifying `content` as required.
7. **Given** a notification was accepted with idempotency key K, **When** the same request with K
   is submitted again (any number of times, including concurrently), **Then** the original
   notification is returned and the provider is called at most once.
8. **Given** the provider returns 5xx, 429, times out or is unreachable, **When** delivery is
   attempted, **Then** the notification moves to `retrying` and is attempted again after an
   exponentially growing, jittered delay.
9. **Given** transient failures persist, **When** the configured maximum attempts (default 5) is
   reached, **Then** the notification becomes `failed` and is placed in the dead-letter area.
10. **Given** the provider returns a 4xx other than 429, **When** delivery is attempted, **Then**
    the notification becomes `failed` immediately with no further attempts.
11. **Given** a notification has been accepted, **When** the API or a worker process crashes at any
    point, **Then** after restart the notification is still delivered (or reaches a terminal state)
    and is not lost.

---

### User Story 2 - Track delivery status (Priority: P1)

An API consumer looks up a notification by id and sees its current status, number of attempts,
the history of each attempt (time, outcome, error), the provider message id and lifecycle
timestamps. For a batch, the consumer sees totals per status and the notifications in the batch.

**Why this priority**: asynchronous delivery is unusable unless consumers can find out what
happened.

**Independent Test**: submit a notification that fails once then succeeds, query it by id, and
verify two attempts with correct outcomes and the final `sent` status.

**Acceptance Scenarios**:

1. **Given** a notification that failed once and then succeeded, **When** queried by id, **Then**
   the response shows status `sent`, attempt count 2, both attempts with timestamp, outcome and
   error (for the failed one), the provider message id and created/updated/sent timestamps.
2. **Given** an unknown notification id, **When** queried, **Then** the system responds 404.
3. **Given** a batch with items in different states, **When** queried by batch id, **Then** the
   response shows a count per status that sums to the number of accepted items, and lists them.

---

### User Story 3 - Absorb bursts with rate-limited, prioritised and scheduled delivery (Priority: P1)

During a flash sale or breaking-news event, consumers submit far more notifications than the
provider may receive. The system accepts them quickly, never exceeds 100 deliveries per second per
channel (across all worker instances combined), delivers high priority before normal before low
when there is a backlog, and holds scheduled notifications until their time.

**Why this priority**: burst handling is the core non-functional promise of the system.

**Independent Test**: submit thousands of notifications for one channel with mixed priorities and
run several workers; measure provider-side arrival rate and arrival order by priority.

**Acceptance Scenarios**:

1. **Given** a backlog of 5,000 SMS notifications and multiple workers, **When** they are
   delivered, **Then** the provider never receives more than 100 SMS deliveries in any one-second
   window.
2. **Given** a backlog at the SMS limit, **When** email notifications are submitted, **Then** email
   delivery proceeds at its own rate, unaffected by the SMS backlog.
3. **Given** a backlog of low and normal priority items, **When** a high priority item is
   submitted, **Then** it is delivered before any remaining normal or low item that has not yet
   started sending.
4. **Given** a notification scheduled 10 minutes in the future, **When** submitted, **Then** its
   status is `scheduled` and it is not delivered before the scheduled time; it is delivered
   promptly after it.
5. **Given** a burst of submissions, **When** the provider is slow, **Then** submission response
   times are unaffected because acceptance never waits on delivery.

---

### User Story 4 - Submit notifications in batches (Priority: P2)

An API consumer submits up to 1,000 notifications in one request and receives a batch id with a
per-item result: accepted (with notification id) or rejected (with reasons). Valid items are
accepted even when others are invalid.

**Why this priority**: bulk campaigns are the main source of bursts; a single call per campaign
is far more efficient for consumers.

**Independent Test**: submit a batch of 10 items with 3 invalid ones and verify 7 accepted, 3
rejected with reasons, and only the 7 delivered.

**Acceptance Scenarios**:

1. **Given** a batch of 1,000 valid items, **When** submitted, **Then** the system responds 202 with
   a batch id and 1,000 accepted results in request order.
2. **Given** a batch of 1,001 items, **When** submitted, **Then** the system responds 400 and
   nothing is stored.
3. **Given** a batch mixing valid and invalid items, **When** submitted, **Then** each valid item is
   accepted and each invalid item is reported by index with field-level reasons.
4. **Given** a batch where every item is invalid, **When** submitted, **Then** the system responds
   400 with per-item reasons and no batch is created.

---

### User Story 5 - Cancel notifications (Priority: P2)

An API consumer cancels a notification that has not started sending yet, or cancels an entire
batch. A cancelled notification is never delivered, even if it was already waiting in the
processing queue.

**Why this priority**: consumers must be able to stop a mistaken or obsolete campaign.

**Independent Test**: submit a scheduled notification, cancel it, advance past the schedule and
confirm the provider never receives it.

**Acceptance Scenarios**:

1. **Given** a notification in `pending`, `scheduled`, `queued` or `retrying`, **When** cancelled,
   **Then** it becomes `cancelled` and the response confirms it.
2. **Given** a notification in `sending`, `sent` or `failed`, **When** cancelled, **Then** the
   system responds 409 with its current status, and the status is unchanged.
3. **Given** a cancelled notification that was already queued for processing, **When** a worker
   picks it up, **Then** it is not sent to the provider.
4. **Given** cancel and send race for the same notification, **When** both happen at once, **Then**
   exactly one wins: either it is cancelled and never delivered, or it is sending/sent and the
   cancel returns 409.
5. **Given** a batch with some items sent and some still queued, **When** the batch is cancelled,
   **Then** all cancellable items become `cancelled` and the response reports counts of cancelled
   and not-cancellable items.
6. **Given** a notification already `cancelled`, **When** cancelled again, **Then** the request
   succeeds with no change (idempotent).

---

### User Story 6 - List and filter notifications (Priority: P2)

An API consumer lists notifications filtered by status, channel and creation date range, newest
first, paging with a cursor.

**Why this priority**: needed to investigate failures and audit campaigns, but less critical than
single-item lookup.

**Independent Test**: create notifications across channels and statuses, list with filters, and
page through results confirming no duplicates or gaps.

**Acceptance Scenarios**:

1. **Given** 250 matching notifications, **When** listed with page size 100, **Then** three pages
   are returned newest first, with a cursor on every page but the last, and no item repeats or is
   skipped.
2. **Given** filters `status=failed` and `channel=email`, **When** listed, **Then** only failed
   email notifications are returned.
3. **Given** a created-at range, **When** listed, **Then** only notifications created within the
   range are returned.
4. **Given** a page size above 100 or an invalid cursor, **When** listed, **Then** the system
   responds 400.

---

### User Story 7 - Operate and monitor the system (Priority: P2)

An operator watches backlog per channel and priority, sent/failed/retried counts and rates per
channel, and end-to-end and provider latency. The operator can trace any request or notification
through the logs by correlation id, and use separate liveness and readiness checks. When a
channel's provider keeps failing, the system pauses that channel automatically and resumes when
it recovers, without affecting other channels.

**Why this priority**: required to run the system in production and to prove the reliability
claims, but it delivers no consumer-visible feature by itself.

**Independent Test**: run a burst with the mock provider failing for one channel, and confirm
metrics, logs by correlation id, readiness and the per-channel pause behave as described.

**Acceptance Scenarios**:

1. **Given** a request with an `X-Correlation-ID` header, **When** it is processed, **Then** every
   log entry for that request and for the resulting notifications' processing carries that id, and
   the id is returned in the response.
2. **Given** a request without the header, **When** processed, **Then** an id is generated, used in
   all related logs and returned in the response.
3. **Given** a backlog, **When** metrics are read, **Then** they show queue depth per channel and
   priority, counts and rates of sent, failed and retried per channel, and latency distributions
   for end-to-end delivery and provider calls.
4. **Given** the system is running but a required dependency is down, **When** checked, **Then**
   liveness reports healthy and readiness reports not ready.
5. **Given** the SMS provider fails repeatedly, **When** the failure threshold is reached, **Then**
   SMS delivery pauses for a cool-down period, paused notifications are not marked failed nor
   consume attempts, email and push continue normally, and SMS resumes automatically once a trial
   delivery succeeds.

---

### User Story 8 - Send by template (Priority: P3)

An API consumer creates a reusable template containing named variables (e.g. "Hi {{.Name}}, your
code is {{.Code}}") and sends notifications by template id plus variable values.

**Why this priority**: convenience feature; consumers can render content themselves today.

**Independent Test**: create a template, send with complete variables and confirm the provider
receives the rendered content; send with a missing variable and confirm a 400.

**Acceptance Scenarios**:

1. **Given** a template with variables Name and Code, **When** a notification is sent with both
   values, **Then** the provider receives the content with values substituted.
2. **Given** the same template, **When** sent without Code, **Then** the system responds 400
   naming the missing variable.
3. **Given** a template whose rendered content exceeds the channel limit, **When** sent, **Then**
   the system responds 400 with the channel length error.
4. **Given** an unknown template id, **When** sent, **Then** the system responds 400 identifying
   the template field.
5. **Given** template syntax that cannot be parsed, **When** the template is created, **Then** the
   system responds 400.

---

### User Story 9 - Receive real-time status updates (Priority: P3)

An API consumer opens a WebSocket subscription for a notification id or batch id and receives a
message every time the status of that notification (or any notification in the batch) changes.

**Why this priority**: improves visibility, but polling the status endpoint already covers the need.

**Independent Test**: subscribe to a notification, submit it, and confirm the subscriber receives
each transition through to `sent` in order.

**Acceptance Scenarios**:

1. **Given** a subscription to a notification, **When** it moves through `queued`, `sending` and
   `sent`, **Then** the subscriber receives one update per transition containing id, new status and
   timestamp.
2. **Given** a subscription to a batch, **When** any item changes status, **Then** the subscriber
   receives an update for that item.
3. **Given** a subscription to an unknown id, **When** opened, **Then** it is rejected with a
   not-found error.
4. **Given** a subscriber connected to one API instance, **When** the status change is caused by a
   worker, **Then** the subscriber still receives the update.

---

### Edge Cases

- Idempotency key reused with a **different** payload → 409 conflict; the original is not changed.
- Two concurrent submissions with the same idempotency key → exactly one notification is created;
  both callers receive it.
- Scheduled time in the past → treated as "deliver now" (status `pending`, not `scheduled`).
- Scheduled time not a valid timestamp → 400.
- Notification delivered to the provider but the system crashes before recording `sent` → it may be
  sent again on recovery (at-least-once); this window is documented and minimised, and duplicates
  are never caused by duplicate submissions or redelivered queue messages for already-sent items.
- Duplicate queue message for a notification already `sent`, `failed` or `cancelled` → ignored.
- Provider returns 429 with a Retry-After hint → next attempt is not earlier than the hint.
- Provider responds with success but an unparsable body → treated as sent, missing message id
  recorded as absent and logged as a warning.
- Provider call exceeds the timeout → counted as a transient failure.
- Unknown channel or priority value → 400.
- SMS content of exactly 160 characters / push content of exactly 4,000 bytes → accepted.
  Multi-byte characters count as one character for SMS and email, and by encoded bytes for push.
- Cancelling a batch where nothing is cancellable → success with zero cancelled.
- Unknown batch id on query or cancel → 404.
- List with an empty result → empty page, no cursor.
- Malformed JSON body or wrong content type → 400 with the standard error shape.
- Circuit paused when the max-attempts budget would otherwise be spent → paused time does not
  count as attempts.

## Requirements *(mandatory)*

### Functional Requirements

**Submission & validation**

- **FR-001**: System MUST accept a notification with: recipient, channel (`sms`, `email`, `push`),
  content, optional priority (`high`, `normal`, `low`; default `normal`), optional scheduled time,
  optional idempotency key, optional template id with variables.
- **FR-002**: System MUST respond 202 with notification id and initial status once the notification
  is durably stored, and MUST NOT wait on delivery.
- **FR-003**: System MUST validate per channel: SMS recipient in E.164 format and content ≤ 160
  characters; email recipient a syntactically valid address and content ≤ 100,000 characters;
  push recipient non-empty and content ≤ 4,000 bytes.
- **FR-004**: System MUST require content unless a template id is supplied; supplying both is
  rejected as ambiguous.
- **FR-005**: System MUST return 400 for invalid input with an error for every invalid field, in a
  single consistent error shape used by all endpoints.
- **FR-006**: System MUST treat an idempotency key as unique: a repeat with the same key and same
  payload returns the original notification; a repeat with a different payload returns 409.
  Neither case creates a new notification or a new delivery.

**Batches**

- **FR-007**: System MUST accept batches of 1 to 1,000 items and reject larger or empty batches
  with 400.
- **FR-008**: System MUST validate each batch item independently, accept valid items, and return a
  batch id plus a per-item result (index, accepted with id, or rejected with field errors). If no
  item is valid, it MUST respond 400 and create no batch.

**Status & listing**

- **FR-009**: System MUST return, for a notification id: status, channel, recipient, priority,
  attempt count, attempt history (time, outcome, error detail), provider message id, batch id if
  any, and created / scheduled / updated / sent timestamps.
- **FR-010**: System MUST return, for a batch id: counts per status and the batch's notifications.
- **FR-011**: System MUST list notifications filtered by any combination of status, channel and
  created-at range, ordered newest first, with opaque cursor pagination; page size default 20,
  maximum 100.

**Lifecycle & cancellation**

- **FR-012**: Notifications MUST follow this lifecycle: `pending` or `scheduled` → `queued` →
  `sending` → `sent` | `retrying` | `failed`; `retrying` → `queued`; and `cancelled` reachable
  only from `pending`, `scheduled`, `queued` or `retrying`. `sent`, `failed` and `cancelled` are
  terminal. Any other transition MUST be rejected.
- **FR-013**: System MUST allow cancelling a notification in a cancellable state and return 409
  with the current status otherwise. Cancelling an already cancelled notification succeeds.
- **FR-014**: System MUST guarantee a cancelled notification is never sent to the provider,
  including when it is already queued; the check is made immediately before sending, atomically
  with entering `sending`.
- **FR-015**: System MUST allow cancelling a batch, cancelling every cancellable item and returning
  counts of cancelled and not-cancellable items.

**Processing**

- **FR-016**: System MUST deliver at most 100 notifications per second per channel, measured across
  all worker instances combined; the limit MUST be configurable.
- **FR-017**: When a backlog exists for a channel, System MUST start high priority deliveries before
  normal, and normal before low.
- **FR-018**: System MUST NOT deliver a scheduled notification before its scheduled time, and MUST
  move it to processing within a few seconds after that time.
- **FR-019**: System MUST send each delivery to the configured provider endpoint as a request
  containing recipient, channel and content, and treat the provider's accepted response as `sent`,
  storing the returned message id.

**Retry & failure handling**

- **FR-020**: System MUST classify provider 5xx, 429, timeouts and network errors as transient, and
  all other 4xx as permanent.
- **FR-021**: System MUST retry transient failures with exponential backoff and random jitter, up
  to a configurable maximum number of attempts (default 5, counting the first), honouring any
  provider Retry-After hint.
- **FR-022**: System MUST mark a notification `failed` and place it in a dead-letter area when
  attempts are exhausted, or immediately on a permanent failure, recording the last error.
- **FR-023**: System MUST apply a time limit to every provider call (configurable).
- **FR-024**: System MUST pause delivery for a channel after repeated consecutive transient
  failures for that channel, resume automatically after a cool-down via a trial delivery, and keep
  other channels unaffected. Notifications held during a pause MUST NOT consume attempts or be
  failed.

**Durability**

- **FR-025**: System MUST NOT lose an accepted notification if the API or any worker crashes or is
  restarted; every accepted notification eventually reaches a terminal state.
- **FR-026**: Delivery MUST be at-least-once and effectively-once: duplicate submissions and
  redelivered work items MUST NOT cause additional provider calls for a notification that has
  already reached a terminal state.
- **FR-027**: On shutdown, System MUST stop taking new work, finish or safely release in-flight
  work, and then exit.

**Templates**

- **FR-028**: System MUST let consumers create a template (name, body with named variables) and
  retrieve it by id; invalid template syntax is rejected with 400.
- **FR-029**: System MUST render the template with supplied variables at submission time, store the
  rendered content on the notification, validate it against channel limits, and reject missing
  variables with 400 naming each one. Unused extra variables are ignored.

**Real-time updates**

- **FR-030**: System MUST let consumers subscribe over WebSocket to a notification id or batch id
  and push an update (notification id, batch id, new status, timestamp) on every status change,
  regardless of which process made the change.

**Observability**

- **FR-031**: System MUST expose metrics: queue depth per channel and priority; sent, failed and
  retried counts per channel (from which rates are derived); end-to-end latency (accepted → sent)
  and provider call latency as distributions.
- **FR-032**: System MUST accept a correlation id from the `X-Correlation-ID` header or generate
  one, return it in the response, store it with each notification, and include it on every log
  entry for that request and for that notification's processing.
- **FR-033**: System MUST expose a liveness check (process is up) and a separate readiness check
  (required dependencies reachable).

**Operability**

- **FR-034**: The full system, including a mock provider able to simulate success, 5xx, 429,
  4xx and slow responses, MUST start locally with one command, and the full test suite MUST run
  with one command.
- **FR-035**: Provider endpoint, rate limit, max attempts, backoff, timeouts and pause thresholds
  MUST be configurable without code changes, with documented defaults.

### Key Entities

- **Notification**: one message to one recipient on one channel. Attributes: id, recipient,
  channel, content (rendered), priority, status, scheduled time, idempotency key, template id,
  batch id, correlation id, attempt count, provider message id, last error, lifecycle timestamps.
- **Delivery Attempt**: one try to hand a notification to the provider. Attributes: notification,
  attempt number, start time, duration, outcome (success / transient / permanent), provider
  status code, error detail.
- **Batch**: a group of notifications submitted together. Attributes: id, created time,
  correlation id, item count; status totals are derived from its notifications.
- **Template**: reusable content with named variables. Attributes: id, name, body, created time.
- **Dead-letter entry**: a notification that ended `failed`, with reason and final attempt, for
  operator inspection.
- **Status Event**: a record that a notification changed status (id, batch id, from, to, time),
  used for real-time subscribers and audit.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: During a burst of 10,000 single submissions, 95% are acknowledged in under 200 ms and
  none waits for delivery to complete.
- **SC-002**: A 1,000-item batch is acknowledged in under 2 seconds.
- **SC-003**: Under sustained backlog, each channel delivers at 90–100 per second and never exceeds
  100 deliveries in any one-second window, regardless of how many worker instances run.
- **SC-004**: In crash tests (killing the API or a worker mid-burst), 100% of accepted
  notifications reach a terminal state after restart; zero are lost.
- **SC-005**: Repeated or concurrent submissions with the same idempotency key produce exactly one
  provider delivery in 100% of test runs.
- **SC-006**: Zero cancelled notifications are delivered across all cancellation and race tests.
- **SC-007**: With a backlog of mixed priorities, every high priority item starts sending before
  any normal or low item that was still waiting when it arrived.
- **SC-008**: No scheduled notification is delivered before its time, and 95% are delivered within
  5 seconds after it (when the channel has no backlog).
- **SC-009**: When one channel's provider is failing, the other channels' throughput and latency
  remain within 10% of normal, and the failing channel resumes within one cool-down period after
  the provider recovers.
- **SC-010**: Real-time subscribers receive status changes within 1 second of the change.
- **SC-011**: An operator can find every log entry for a given request or notification using only
  its correlation id.
- **SC-012**: A reviewer can go from a fresh clone to a running system with one command and run
  the full test suite with one more, in under 10 minutes on a typical laptop.

## Assumptions

- Consumers are trusted internal services; authentication, authorisation and multi-tenancy are out
  of scope, so idempotency keys are unique system-wide rather than per consumer.
- "Sent" means the provider accepted the message; actual handset/inbox delivery receipts are out of
  scope.
- The 100/second limit counts every provider call, including retries, and applies per channel
  independently.
- Priority ordering is best-effort across the waiting backlog; a delivery already in progress is
  never pre-empted.
- Default backoff: base delay 1 second, doubling per attempt, capped at 5 minutes, with full jitter.
- Default provider call timeout: 5 seconds. Default channel pause: after 5 consecutive transient
  failures, for 30 seconds, then one trial delivery.
- The dead-letter area is inspectable via the status and list endpoints (status `failed`);
  re-driving failed notifications is out of scope.
- Template bodies are channel-agnostic; channel limits apply to the rendered result. Template
  update and delete are out of scope.
- Email content is plain text; HTML rendering, subjects and attachments are out of scope.
- Data retention: notifications and attempts are kept indefinitely for this version.
- WebSocket subscriptions deliver changes from the moment of subscribing; replay of earlier
  changes is not required (current status is available from the status endpoint).
- The external provider is reachable over HTTP; in demos it is a public request-inspection
  endpoint and in tests a local mock that can simulate failures.
- Out of scope: real SMS/email/push gateways, user preferences/opt-out, cloud deployment.
- Templates (US8) and real-time updates (US9) are treated as lower priority and are built only
  after US1–US7 are complete and tested.
