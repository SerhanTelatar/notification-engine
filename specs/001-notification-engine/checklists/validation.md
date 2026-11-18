# Required-scope validation

Results of running the quickstart scenarios and the SC-001 load check against
`docker compose up --build` (one api, one worker, the mock provider) on a developer laptop
(Docker Desktop on macOS, 4 GB VM), 2026-11-18. Commit under test: branch `001-notification-engine`.

## Load check (T099, SC-001)

`make loadtest`: vegeta v12.12.0 fires 10,000 single `POST /api/v1/notifications`, each with a
unique SMS recipient, content and idempotency key, at 500/s from inside the Compose network.

| Requests | Rate | Success | p50 | p95 | p99 | max |
|---------:|-----:|--------:|----:|----:|----:|----:|
| 10,000 | 500/s | 100% (all 202) | 0.7 ms | **9.0 ms** | 344.5 ms | 514.3 ms |

- [x] **SC-001: PASS.** p95 = 9.0 ms < 200 ms, and no request waits for delivery: the API only
  inserts the row, and the worker drained the 10,000 SMS afterwards at the rate limit.
- Observation: the tail above p95 (p99 = 344 ms) comes from the first second of the run, while
  the connection pools warm up and the worker's relay starts on the same Postgres. It doesn't
  affect SC-001, which is set on p95.
