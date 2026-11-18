# Contract: Observability (metrics, logs, health)

Dashboards and alerts depend on these names and labels; treat a change to them as a contract
change.

## Prometheus metrics

All latencies are histograms (Constitution IV). Label values are bounded: channel ∈ 3, priority ∈ 3,
status ∈ 8, and outcome ∈ {success, transient, permanent, deferred, abandoned}.

| Metric | Type | Labels | Emitted by | Meaning |
|--------|------|--------|-----------|---------|
| `notifications_created_total` | counter | channel, priority | api | Notifications accepted (replays not counted) |
| `notifications_rejected_total` | counter | reason | api | Validation failures |
| `notifications_delivered_total` | counter | channel, outcome | worker | Outcomes of provider calls; rates via `rate()` |
| `notification_retries_total` | counter | channel | worker | Transitions to `retrying` |
| `notifications_dead_lettered_total` | counter | channel, reason | worker | Transitions to `failed`; reason ∈ {exhausted, permanent, provider_unavailable} |
| `notification_backlog` | gauge | channel, priority, status | worker (sampled every 5 s from DB) | Queue depth per channel/priority (FR-031) |
| `rabbitmq_queue_messages` | gauge | queue | worker (passive declare every 5 s) | Broker-side depth |
| `provider_request_duration_seconds` | histogram | channel, outcome | worker | Provider call latency |
| `notification_e2e_latency_seconds` | histogram | channel, priority | worker | `sent_at − max(created_at, scheduled_at)` |
| `rate_limiter_wait_seconds` | histogram | channel | worker | Time spent waiting for a permit |
| `rate_limiter_timeouts_total` | counter | channel | worker | Messages requeued because no permit was obtained within `LIMITER_MAX_WAIT` |
| `circuit_breaker_state` | gauge | channel | worker | 0 closed, 1 half-open, 2 open |
| `relay_published_total` | counter | channel | worker | Messages published by the relay |
| `relay_backpressure_skips_total` | counter | channel | worker | Relay ticks that skipped a channel because its `queued` count was at `RELAY_MAX_INFLIGHT_PER_CHANNEL` |
| `reaper_recovered_total` | counter | kind (sending, queued) | worker | Crash-recovery actions |
| `http_request_duration_seconds` | histogram | route, method, code | api | `route` is the chi route pattern, never the raw path |
| `ws_connections` | gauge | — | api | Open WebSocket subscriptions |

Histogram buckets:

- provider: `0.01 … 10 s`
- e2e: `0.05 … 600 s`
- http: Prometheus defaults

## Logs

These fields appear on every line (slog JSON to stdout):

| Field | Always | Notes |
|-------|--------|-------|
| `time`, `level`, `msg` | yes | slog defaults |
| `service` | yes | `api` or `worker` |
| `correlation_id` | on every request and message path | from `X-Correlation-ID`, or generated |
| `notification_id`, `batch_id` | when known | |
| `channel`, `status`, `attempt` | in the worker | |
| `error` | on failures | wrapped error chain; never logged in an HTTP response body |

Recipient and content are **not** logged (they are PII-like). The HTTP access log has `method`,
`route`, `status`, `duration_ms` and `bytes`.

## Health endpoints

| Endpoint | api | worker (`OPS_ADDR`) |
|----------|-----|---------------------|
| `GET /healthz` | 200 while the process is running | same |
| `GET /readyz` | Postgres required; Redis optional (`degraded` keeps 200) | Postgres, RabbitMQ and Redis all required; 503 if any is down |
| `GET /metrics` | yes | yes |

Each dependency check has a 1 s timeout. The response body is the `Health` schema in
openapi.yaml.
