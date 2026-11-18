# Required-scope validation

These are the results of running the quickstart scenarios #1–#15 (T100) and the SC-001 load check
(T099) against `docker compose up --build`.

**Environment**:
- A developer laptop: Docker Desktop on macOS, 4 GB VM.
- One api, the mock provider, and one worker (two workers with `--scale worker=2` from #11 on).
- Date: 2026-11-18. Branch `001-notification-engine`.

**Builds**:
- Scenarios #1–#14 ran on the build after T120.
- T100 found a gap in #15, fixed by T121/T122 (worker supervisor). #15 and #11 were then rerun on
  that build.

## Summary

| # | Scenario | Result |
|---|----------|--------|
| 1 | Create + deliver | ✅ |
| 2 | Validation | ✅ |
| 3 | Idempotency | ✅ |
| 4 | Transient retry → failed | ✅ |
| 5 | Permanent failure | ✅ |
| 6 | Batch partial | ✅ |
| 7 | Batch limits | ✅ |
| 8 | Cancel | ✅ |
| 9 | Cancel 409 | ✅ |
| 10 | List | ✅ |
| 11 | Burst + rate limit, 2 workers | ✅ after T120 (the second worker couldn't start before it) |
| 12 | Channel isolation + breaker | ✅ |
| 13 | Crash safety | ✅ (one duplicate provider call: the documented at-least-once window) |
| 14 | Correlation | ✅ |
| 15 | Readiness with RabbitMQ stopped | ✅ after T121/T122 (the worker exited before them) |
| — | SC-001 load check | ✅ p95 9.0 ms and 2.3 ms in two runs |

## Load check (T099, SC-001)

`make loadtest` uses vegeta v12.12.0, running inside the Compose network. It fires 10,000 single
`POST /api/v1/notifications` at 500/s, each with a unique SMS recipient, content and idempotency
key.

| Run | Requests | Success | p50 | p95 | p99 | max |
|-----|---------:|--------:|----:|----:|----:|----:|
| 1 (fresh stack) | 10,000 | 100% (all 202) | 0.7 ms | **9.0 ms** | 344.5 ms | 514.3 ms |
| 2 (after T122, 2 workers) | 10,000 | 100% (all 202) | 0.7 ms | **2.3 ms** | 31.3 ms | 99.2 ms |

- [x] **SC-001: PASS.** p95 is well under 200 ms in both runs.
- No request waits for delivery: the API only inserts the row, and the worker drained the SMS
  afterwards at the rate limit.
- The first run's p99 tail didn't reproduce in the second run. I didn't investigate it further
  because SC-001 is set on p95. A single run on a laptop VM isn't a capacity measurement.

## Scenario details

- [x] **#1 Create + deliver.**
  - The create returned 202 `pending`.
  - The notification was `sent` 2 s later, with `attempts: 1`, `provider_message_id: mock-1`, and
    one attempt in its history.
- [x] **#2 Validation.** An SMS to `12345` with 161 characters of content returned 400
  `validation_failed`, and `details` listed both `recipient` (E.164) and `content` (161 > 160).
- [x] **#3 Idempotency.**
  - Sending twice with `Idempotency-Key: k1` returned the same id both times; the second response
    had `Idempotent-Replayed: true`.
  - The same key with different content returned 409 `idempotency_conflict`.
  - The mock received exactly one call.
- [x] **#4 Transient retry → failed.**
  - The mock was set to 503.
  - The notification was `failed` after 11 s, with 5 `transient` attempts at jittered gaps of 0.4,
    0.79, 0.61 and 7.98 s.
  - `last_error` was `provider returned HTTP 503: …`.
  - `notifications.dlq` held 1 message, matching the management API and the worker's
    `rabbitmq_queue_messages` gauge.
  - `notifications_dead_lettered_total{sms,exhausted}` was 1.
  - The 5 consecutive failures also tripped the SMS breaker (`circuit_breaker_state{sms}=2`).
- [x] **#5 Permanent failure.**
  - The mock was set to 400. The scenario used email, because the SMS breaker was still open
    from #4.
  - The notification was `failed` with `attempts: 1`.
- [x] **#6 Batch partial.**
  - A batch of 10 items with 3 invalid returned 202, `accepted: 7`, `rejected: 3` at indexes
    7–9.
  - `GET /batches/{id}` showed a total of 7.
  - The 7 SMS were first deferred as `retrying`, because the breaker from #4 was still open. They
    were later `sent` with `attempts: 1`, so the deferral used no attempts (FR-024).
- [x] **#7 Batch limits.** 1,001 items returned 400, and 3 all-invalid items returned 400 listing
  every item's error. The `batches` row count was unchanged (1 before, 1 after).
- [x] **#8 Cancel.** A notification scheduled 10 minutes ahead was cancelled with 200 `cancelled`;
  the second cancel also returned 200 `cancelled`. The mock never received it.
- [x] **#9 Cancel 409.** Cancelling #1's `sent` notification returned 409 `not_cancellable`
  with `current_status: sent`.
- [x] **#10 List.**
  - `channel=sms&status=sent&limit=2` produced 5 pages of 9 items: newest first, no repeats, and
    every item `attempts: 1`.
  - `limit=101` returned 400.
- [x] **#11 Burst + rate limit with 2 workers** (`make demo-burst`, 5,000 SMS: 10% high, 30%
  normal, 60% low).
  - **First attempt failed**: `--scale worker=2` didn't start the second worker
    (`port is already allocated` on 9090). Fixed by T120 with the `9090-9099:9090` port range.
  - **Rate**: 95.1/s on average, and **at most 96 in any sliding 1 s window** at the provider
    (limit 100).
  - **Priority**: strict. The 500 high items were delivered at positions 0–499, normal at
    500–1999, and low at 2000–4999.
  - **Backpressure**: SMS `queued` peaked at 995 (sampled gauge), never above 1,000.
  - `relay_backpressure_skips_total` stayed 0: each tick tops up a small partial quota instead of
    skipping, as `TestRelayBackpressureBurst` also documents. quickstart.md #11 was corrected,
    since it expected skips to rise.
  - **Rerun on the T122 build**: 94.9/s, peak 96. Both workers' relays published (3,012 and 1,989
    SMS).
- [x] **#12 Channel isolation + breaker.**
  - The mock returned 503 for SMS only during a batch of 600 SMS, 300 email and 300 push.
  - All 600 email and push were `sent` within 5 s.
  - `circuit_breaker_state{channel="sms"}` was 2 on both workers, while email and push stayed 0.
  - SMS attempts stayed frozen at 14 over 10 s while the breaker was open, with all 600 SMS
    `retrying`.
  - After the control was cleared, all 600 SMS were `sent` within 24 s (cool-down 30 s), with at
    most 2 attempts each.
- [x] **#13 Crash safety.**
  - Both workers were killed (`docker compose kill worker`) 6 s into a 2,000-SMS burst. At that
    moment the rows were: pending 405, queued 991, sending 7, sent 597.
  - After `up`, all 2,000 were `sent` within 121 s (`REAPER_SENDING_TIMEOUT` is 2 min). Nothing
    was lost.
  - `reaper_recovered_total{kind="sending"}` was 7, exactly the rows that were in `sending`.
  - The provider saw 2,001 calls for 2,000 notifications: **one duplicate**. A call the provider
    accepted just before the kill was retried after recovery. That's the documented at-least-once
    window (ADR 0003, README trade-offs). The provider receives the notification id as its
    `Idempotency-Key`, which lets a deduplicating provider absorb it.
- [x] **#14 Correlation.**
  - A notification was created with `X-Correlation-ID: demo-14`.
  - `docker compose logs api worker` showed `notification accepted` and `http request` (api) and
    `notification sent` (worker), all with `correlation_id: demo-14`.
  - The recipient and content appear in no log line.
  - The relay's `notification queued` line is debug level, so it doesn't show at `LOG_LEVEL=info`.
- [x] **#15 Readiness with RabbitMQ stopped.**
  - **First attempt failed**: the worker process exited when the broker connection closed (a
    consumer error stopped every component). Compose restarted it in a loop, so `/healthz` and
    `/readyz` were unreachable rather than 200 and 503. No data was lost and the API kept
    accepting, but US7-AS4 wasn't met.
  - T089's readiness test had only checked the readiness policy, not the process, so it missed
    this. T121 added a supervisor integration test (red) and T122 the fix: a RabbitMQ supervisor
    that redials and restarts the broker session while the ops server stays up.
  - **Rerun with 2 workers**: worker `/readyz` 503 with `rabbitmq: unavailable`, `/healthz` 200,
    and both workers stayed up with no restarts.
  - The API's `/readyz` stayed 200, and a create during the outage was stored as `pending`.
  - After `docker compose start rabbitmq`, worker readiness returned to 200 in 22 s (broker
    startup plus redial backoff, capped at 15 s). The notification created during the outage was
    `sent` 1 s later.
  - Each worker logged `rabbitmq connection lost` once and then `rabbitmq session started` again.

## Gaps found and fixed (appended to tasks.md)

- **T120**: the worker's fixed host port prevented `--scale worker=2`. Fixed with a published
  port range (`a154f78`).
- **T121/T122**: the worker exited on broker loss instead of reporting not ready. Fixed with a
  connection supervisor (`c07414b` red, `1c094f1` green).

## Notes

- With a port range, Docker assigns any free port in 9090–9099 to each replica, and a restart can
  move it. `docker compose port --index N worker 9090` finds it. README and quickstart.md now say
  so.
- Scenarios #16–#18 (templates, WebSocket, metrics summary) belong to the bonus stories and weren't
  run.
