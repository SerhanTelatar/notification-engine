# Contract: External Provider (outbound) and Mock Provider

## Outbound request (worker → `PROVIDER_URL`)

```http
POST {PROVIDER_URL}
Content-Type: application/json
Idempotency-Key: <notification id>
X-Correlation-ID: <correlation id>

{"to": "+905551234567", "channel": "sms", "content": "Your code is 1234"}
```

- Timeout: `PROVIDER_TIMEOUT` (default 5 s) for the whole request.
- Redirects are not followed, so a 3xx is classified as permanent.
- The response body is read up to 64 KiB.

## Response classification (research R9)

| Response | Outcome | Effect |
|----------|---------|--------|
| 2xx (202 expected; webhook.site returns 200) | success | `sent`, `provider_message_id = body.messageId` (null if the body can't be parsed; a warning is logged) |
| 429 | transient | retry; `Retry-After` (seconds or HTTP-date) sets the minimum delay |
| 5xx | transient | retry with backoff |
| timeout, connection refused/reset, DNS failure | transient | retry with backoff |
| any other status (4xx ≠ 429, 1xx, 3xx) | permanent | `failed` immediately and dead-lettered; does not trip the breaker |

Expected success body:

```json
{"messageId": "a1b2c3", "status": "accepted", "timestamp": "2026-11-18T10:00:00Z"}
```

## Mock provider (`tools/mockprovider`, used by docker-compose and tests)

`POST /send`: behaves like the provider. The behaviour is set with env vars or per request:

| Control | Env default | Per-request header override |
|---------|-------------|-----------------------------|
| failure rate (0–1) of 503 responses | `MOCK_FAILURE_RATE=0` | `X-Mock-Failure-Rate` |
| forced status code | — | `X-Mock-Status: 429` |
| added latency | `MOCK_LATENCY=20ms` | `X-Mock-Latency: 6s` |
| Retry-After on a 429 | `MOCK_RETRY_AFTER=1` | — |

The mock has control endpoints for demos. Integration tests use an `httptest` server instead:

- `PUT /control` with a JSON body `{failure_rate, status, latency, channel}` changes the
  behaviour at runtime. It can be scoped to one channel, which is used for the breaker-isolation
  demo.
- `GET /received` returns the requests received, with timestamps, for rate and order checks.
- `DELETE /received` clears them.

The per-request headers are only for manual curl experiments; the worker never sends them.
