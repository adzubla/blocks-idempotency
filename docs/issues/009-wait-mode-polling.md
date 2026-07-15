# Slice 009 — WAIT mode (core polling) + `response_unavailable`

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

`whenInProgress=WAIT`: the waiter **polls** the store (~100ms with jitter, bounded by
`wait-timeout`) until the key completes (→ replay), the timeout elapses (→ 409 +
`Retry-After`), or the key disappears (→ 409 + `Retry-After`) — the polling model of
ADR 0002. Also the `response_unavailable` path: a completed record with no
replayable body (effect done but body absent, including a 2xx body over
`max-body-size` at completion time) → **409 without `Retry-After`**, reason
`response_unavailable` (terminal, no re-execution).

Scope note: this is the polling model against the in-memory fake. Postgres native
locking (which safely self-promotes on primary rollback) is a later batch.

## Acceptance criteria

- [x] A WAIT waiter replays once the primary completes 2xx.
- [x] A WAIT waiter returns **409 + `Retry-After`** on wait-timeout and on key-gone.
- [x] Polling uses jitter and is bounded by `wait-timeout`.
- [x] Completed-without-body → **409 `response_unavailable`**, no `Retry-After`,
      effect not re-run.
- [x] A 2xx body over `max-body-size` is not cached; the record is marked
      completed-without-body.
- [x] `IdempotencyEngine` tests cover every WAIT outcome and `response_unavailable`.

## Blocked by

- Slice 001 — header-strategy happy-path replay
- Slice 003 — in-flight concurrency → 409 REJECT
