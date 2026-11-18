# ADR 0006: Keyset (cursor) pagination for listing notifications

- **Status**: Accepted
- **Date**: 2026-11-18
- **Source**: research.md R13

## Context

`GET /api/v1/notifications` lists notifications newest first, filtered by status, channel and a
created-at range, with a configurable page size of at most 100 (FR-011). Paging must not repeat
or skip items, even while new notifications keep arriving during a burst (US6-AS1). The table
grows by thousands of rows per minute, so deep pages must stay as cheap as the first.

## Decision

- **Keyset pagination on `(created_at, id)`**, ordered `created_at DESC, id DESC`.
  - The next page continues strictly after the last row with the row comparison
    `(created_at, id) < ($cursor_created_at, $cursor_id)`.
  - `id` (UUIDv7) breaks ties between rows created in the same microsecond.
- **Opaque cursor**: URL-safe base64 (no padding) of `{"c": created_at, "i": id}`, returned as
  `next_cursor`.
  - It's absent on the last page.
  - The use case fetches `limit + 1` rows to know whether another page exists.
  - A malformed cursor is a 400 on `cursor`.
- **Filters** are optional parameters (`sqlc.narg`, NULL when absent): `from` is inclusive, `to`
  exclusive. `limit` defaults to 20, and values outside 1–100 are a 400 (not silently clamped),
  consistent with the rest of the validation.
- **Indexes**: `notifications_list_idx (created_at DESC, id DESC)` and
  `notifications_list_filtered_idx (channel, status, created_at DESC, id DESC)`.
- **Unsigned**: the cursor carries no HMAC. Editing a well-formed cursor only moves the starting
  position, which a client can already do with `from` and `to`, so signing would add key
  management without protecting anything.

## Alternatives considered

- **Offset/limit**: rows inserted between page requests shift every later page (duplicates and
  gaps), and `OFFSET n` scans and discards n rows, so deep pages get slower.
- **Cursor on `id` alone**: UUIDv7 is time-ordered, but only to millisecond precision with random
  bits after that. `created_at` is the field the API sorts and filters on, so it leads the key.
- **Server-side cursors or snapshots**: they hold resources per client and don't fit a stateless
  API behind a load balancer.
- **Signed (HMAC) cursors**: see above; no security benefit here.

## Consequences

- Page cost doesn't depend on depth. Inserts during paging appear only on a fresh first page,
  never as duplicates or gaps. The integration test verifies this by adding 30 rows mid-paging,
  with pages coming out 100/100/50 and no repeats.
- Clients can't jump to page N; they follow `next_cursor`. That suits the operator and
  investigation use cases (US6).
- A cursor stays valid indefinitely, because it encodes a position rather than server state.
- The `IS NULL OR …` form of the optional filters keeps one query for every combination. At much
  larger scale, dedicated queries per filter combination would let the planner use the filtered
  index more precisely.
