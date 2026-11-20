# Slice 065 — Postgres: blocked duplicate requests can exhaust the connection pool

> Source: system analysis (2026-11-19) · Type: AFK
> Status: ready-for-agent

## What to build

With the Postgres store, every in-flight `@Idempotent` request holds a pooled
connection for its handler's whole duration (the reservation *is* the open
transaction, ADR 0003). Every concurrent **duplicate** also takes a connection
and blocks on the row lock for up to `lock-timeout` (2s default) in `reserve()`.
Under `WAIT` it then blocks again in `await()` for up to `wait-timeout`.

A burst of retries for slow requests (for example a client retry storm after a
timeout) can therefore hold the whole Hikari pool. Unrelated requests that need
a connection then stall or time out, which turns a slow endpoint into an
app-wide outage.

Candidate responses, for triage:
- Document the sizing rule (pool size vs. expected concurrent primaries plus
  blocked duplicates) and recommend a short `lock-timeout` for `REJECT`
  endpoints.
- Optionally bound the number of concurrently blocked waiters, for example with
  a semaphore, failing fast with 409 + `Retry-After` when it's full.
- Add a metric or gauge for blocked waiters so operators can see the pressure.

## Acceptance criteria

- [x] Triage decision recorded (document / bound waiters / metric).
- [x] README's Postgres section covers connection-pool sizing for this store.

## Blocked by

—

## Comments

- 2026-11-20 — Triage: **document only**. README §6.4 now explains how
  primaries and blocked duplicates each hold a connection, the budget to size
  for, the `lock-timeout` trade-off on `REJECT` endpoints, preferring `REJECT`
  over `WAIT` for slow handlers, and which pool metrics to watch. A waiter cap
  or gauge can be revisited if the guidance proves insufficient.
