# README backlog

Notes collected during implementation for the README (task T098). Each item becomes README content
when T098 is done; this file is then deleted.

## Bugs caught by tests

- **Consumer claim raced the relay commit** (fixed in `be59a7b`).
  - **The race**: the relay publishes inside its transaction (ADR 0002), so a message can reach a
    consumer before the commit.
  - **The bug**: the original claim was a single guarded `UPDATE … WHERE status='queued'`. Under
    READ COMMITTED that UPDATE picks its candidate rows from its snapshot, where the row was still
    `pending`. It matched 0 rows *without waiting*, and the message was dropped. The row was then
    committed as `queued` with no message and sat there until the 15-minute reaper republished
    it.
  - **How it was caught**: the delivery integration tests (T043) failed whenever a warm consumer
    beat the commit. Each test passed alone and failed when run after another.
  - **The fix**: claim with `SELECT … FOR UPDATE` (no status predicate) and then the guarded
    update, in one transaction. The locking read waits for the relay's lock and sees the committed
    status.
  - **Guard**: `TestRepoClaimWaitsForAnOpenRelayTransaction` holds a relay transaction open while
    claiming, and it fails without the fix. Research R4 and ADR 0002 were corrected.
  - **Lesson for the README**: integration tests against real Postgres caught an
    isolation-level assumption that unit tests with fakes could not.

- **Rate limiter could fail open at its deadline** (fixed in `b7bc154`).
  - **The bug**: right after a context deadline passes, `ctx.Err()` can still be nil for a
    moment. `Acquire` returned `(zero, nil)` there, which the deliverer read as a granted permit,
    so a notification could be sent *without a permit* while Redis was down.
  - **How it was caught**: the Redis-outage integration test (traffic cut through a TCP proxy)
    failed 1 time in 10.
  - **The fix**: return `DeadlineExceeded` explicitly. The deliverer also rejects a permit
    without a slot time.
  - **Guard**: a unit test with a context whose deadline has passed but whose `Err()` is nil.
    The outage test then passed 15/15.
- **Exactly spaced permits still bunched at the provider** (fixed in `b7bc154`).
  - **What was seen**: `demo-burst` on the compose stack measured 106 calls in a sliding second
    against a limit of 100, although the permits were exactly 10 ms apart.
  - **The cause**: claim latency between the permit and the call, plus connection setup and host
    jitter.
  - **The fix**: pace each call on its slot plus a constant offset, size the provider connection
    pool, and add 5% headroom. Measured peak 96–98. See ADR 0004.
  - **Lesson**: measure the limit where the spec measures it (at the provider), not where the
    code enforces it.
