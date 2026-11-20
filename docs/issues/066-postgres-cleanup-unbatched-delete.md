# Slice 066 — Postgres cleanup sweep is a single unbatched `DELETE`

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

`PostgresIdempotencyCleanupJob` runs
`DELETE FROM idempotency_record WHERE expires_at < clock_timestamp()` as one
statement, on a fixed delay, on **every** application instance.

At high write volume with the 24h default TTL, a sweep can delete a large
number of rows in one transaction. That means long-running statements, write-ahead-log
(WAL) and replication-lag spikes, and table bloat until autovacuum catches
up. N instances run the same sweep concurrently and contend for the same rows.
The `DELETE` can also block on a row that an open reservation is currently
superseding (its `UPDATE` holds the row lock), so a sweep can stall for as long
as a slow handler runs.

Candidate fixes, for triage:
- Delete in bounded batches, e.g. `DELETE ... WHERE ctid IN (SELECT ctid ...
  WHERE expires_at < clock_timestamp() LIMIT n FOR UPDATE SKIP LOCKED)`, looping
  until a batch comes back short. `SKIP LOCKED` also removes both the
  cross-instance contention and the blocking on rows held by open reservations.
- Optionally make the sweep leader-only, or document running it on one instance.

## Acceptance criteria

- [ ] The sweep deletes in bounded batches and never waits on a row held by an open reservation.
- [ ] Concurrent sweeps from several instances don't contend or block one another.
- [ ] Existing cleanup tests still pass.

## Blocked by

—
