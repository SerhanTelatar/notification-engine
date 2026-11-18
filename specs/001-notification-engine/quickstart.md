# Quickstart & Validation Guide

This guide shows that the feature works end to end. The request and response shapes are in
[contracts/openapi.yaml](./contracts/openapi.yaml), and the mock provider controls are in
[contracts/provider.md](./contracts/provider.md).

## Prerequisites

- Docker with Compose v2. That's all you need: the Go toolchain and codegen tools also run in
  containers when `DOCKER=1` is set (research R1).
- Optional: a local Go install (version from `go.mod`), plus `curl`, `jq` and `websocat` for the
  manual checks below.

## Run

```bash
cp .env.example .env              # defaults work as-is
docker compose up --build         # postgres, rabbitmq, redis, migrate, api, worker, mockprovider
docker compose --profile observability up   # + prometheus, grafana, jaeger (bonus)
```

| URL | What |
|-----|------|
| http://localhost:8080/swagger | API docs |
| http://localhost:8080/readyz · http://localhost:9090/readyz | API / worker readiness (with several workers: `docker compose port --index N worker 9090`) |
| http://localhost:15672 (guest/guest) | RabbitMQ UI |
| http://localhost:3000 | Grafana (observability profile) |

To use webhook.site as the provider in a demo, set `PROVIDER_URL=https://webhook.site/<uuid>` in
`.env` and restart the worker.

## Test

```bash
make test        # unit + integration, with -race (testcontainers; needs Docker)
make test-unit   # fast, no Docker
make lint
```

Expected result: every package passes with `-race`. The integration tests cover each scenario
listed in the plan's test matrix.

## Manual validation scenarios

The API is at `API=http://localhost:8080/api/v1`.

| # | Scenario (spec ref) | Action | Expected |
|---|---------------------|--------|----------|
| 1 | Create + deliver (US1) | `POST $API/notifications` with an SMS to `+905551234567` | 202, `status: pending`; a GET a moment later shows `sent`, `attempts: 1`, `provider_message_id` set |
| 2 | Validation (US1-AS3..6) | an SMS to `12345` with 161 characters of content | 400, `validation_failed`, `details` lists both `recipient` and `content` |
| 3 | Idempotency (US1-AS7) | send #1 twice with `Idempotency-Key: k1`, then once with k1 and different content | the same id twice (second has `Idempotent-Replayed: true`); then 409 `idempotency_conflict`; the mock's `GET /received` shows a single call |
| 4 | Transient retry → failed (US1-AS8/9) | `PUT mock /control {"status":503}`, then create | `attempt_history` grows with jittered gaps, then `failed` after 5 attempts; one message in `notifications.dlq` |
| 5 | Permanent failure (US1-AS10) | `PUT mock /control {"status":400}`, then create | `failed`, `attempts: 1` |
| 6 | Batch partial (US4) | a batch of 10 items, 3 invalid | 202, `accepted: 7`, `rejected: 3` with indexes; `GET /batches/{id}` counts add up to 7 |
| 7 | Batch limits (US4-AS2/4) | 1,001 items; then 3 items that are all invalid | 400 for both, and no batch is created |
| 8 | Cancel (US5) | create with `scheduled_at` = now + 10 min, cancel it, cancel it again | 200 `cancelled` both times; `GET /received` never shows it |
| 9 | Cancel 409 (US5-AS2) | cancel the `sent` notification from #1 | 409 `not_cancellable`, `current_status: sent` |
| 10 | List (US6) | `GET $API/notifications?channel=sms&status=sent&limit=2`, then follow `next_cursor` | newest first; no repeats across pages; `limit=101` → 400 |
| 11 | Burst + rate limit (US3, SC-003) | `make demo-burst` (5,000 SMS, mixed priority; 2 workers via `--scale worker=2`) | `GET mock /received` shows ≤ 100 SMS in any 1 s window; high-priority items are sent first; `notification_backlog{channel="sms",status="queued"}` peaks at about 1000 and never above it. `relay_backpressure_skips_total` may stay 0: each relay tick usually tops up a small partial quota instead of skipping |
| 12 | Channel isolation + breaker (US7-AS5) | `PUT mock /control {"status":503,"channel":"sms"}` during a burst | `circuit_breaker_state{channel="sms"}=2`; email/push keep flowing; SMS `attempts` don't grow while open; after the control is cleared, SMS resumes within about 30 s |
| 13 | Crash safety (SC-004) | `docker compose kill worker` mid-burst, then `up worker` | every notification reaches a terminal status; `reaper_recovered_total` > 0 if any were in `sending` |
| 14 | Correlation (US7-AS1) | send #1 with `X-Correlation-ID: demo-1`, then `docker compose logs api worker \| grep demo-1` | the api and worker lines for that notification all carry `demo-1` |
| 15 | Readiness (US7-AS4) | `docker compose stop rabbitmq` | worker `/readyz` → 503; worker `/healthz` → 200; api still accepts notifications |
| 16 | Templates (US8) | create the template `Hi {{.Name}}, your code is {{.Code}}`; send with both variables, then without `Code` | the provider receives the rendered text; the second send is a 400 naming `Code` |
| 17 | Real-time (US9) | `websocat "ws://localhost:8080/ws?notification_id=<id>"` around a create | a `snapshot`, then `queued` → `sending` → `sent` |
| 18 | Metrics (US7-AS3) | `curl :9090/metrics \| grep notification_backlog`; `GET $API/metrics/summary` | gauges per channel/priority/status; p50/p95/p99 latency in the summary |
