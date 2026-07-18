# Slice 032 — Redis silently drops a slow primary's response once `lock-ttl` passes

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: needs-triage

## What to build

Redis lifecycle rides the record key's own TTL. When a legitimately-slow
primary runs longer than its `lock-ttl`, the record key has already expired
(been removed) by the time `complete()` runs: the Lua script's `HGET token`
returns nil, `COMPLETE_SCRIPT` no-ops (`RedisIdempotencyStore.java:84-97`), and
**the response is silently dropped — never cached**, with no signal to the
caller (`complete()` returns normally).

Postgres and the in-memory store do **not** have this: they keep the
reservation alive across a slow handler (Postgres via its still-open
transaction; in-memory via logical, not physical, expiry) and cache the
response. So the same slow-handler scenario yields different outcomes per store.

This is the **documented best-effort vs. exactly-once split**, not a contract
violation — the class javadoc calls Redis *"Best-effort"* (`RedisIdempotencyStore.java:31`)
and the SPI javadoc states *"redis is best-effort; postgres is exactly-once"*
(`IdempotencyStore.java:14-16`). `lock-ttl` is deliberately the mechanism that
reclaims a **crashed** primary's key; a slow-but-alive primary losing its cache
slot is the flip side of that TTL-based reclamation. Filed here as a **tracked
limitation** (there may be nothing to "fix" in code).

Candidate responses, for triage:
- **Document** the requirement that `lock-ttl` comfortably exceed worst-case
  handler duration for the Redis store (README / config guidance), since
  exceeding it silently disables caching (not just crash-recovery).
- Optionally have `complete()` surface (log/metric) when it no-ops because the
  reservation is gone, so an operator can see the caching gap rather than it
  being invisible.
- Or accept as-is and mark `wontfix` (a legitimate outcome for a documented
  best-effort guarantee).

## Acceptance criteria

- [x] A disabled repro exists:
      `RedisSlowPrimaryBestEffortTest.aSlowPrimaryPastItsLockTtlStillHasItsResponseCached`
      (`@Disabled`, `@Testcontainers`; expiry simulated by deleting the key).
      Removing `@Disabled` fails today because the response is dropped.
- [ ] Triage decision recorded: document the `lock-ttl` guidance, add a
      no-op-on-complete signal, or `wontfix` with rationale.
- [ ] If kept as-is, `## Comments` captures why (documented best-effort
      tradeoff), mirroring the existing declined slices.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). This is
  a **documented limitation note**, not a defect against the store contract —
  raised so the tradeoff (and its config implication) is tracked rather than
  tribal knowledge.
