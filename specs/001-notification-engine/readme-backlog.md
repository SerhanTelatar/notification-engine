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
