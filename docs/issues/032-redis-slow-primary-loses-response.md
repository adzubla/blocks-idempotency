# Slice 032 — Redis silently drops a slow primary's response once `lock-ttl` passes

> Source: bug hunt (2026-07-17) · Type: AFK
> Status: wontfix

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
- [x] Triage decision recorded: document the `lock-ttl` guidance, add a
      no-op-on-complete signal, or `wontfix` with rationale.
- [x] If kept as-is, `## Comments` captures why (documented best-effort
      tradeoff), mirroring the existing declined slices.

## Blocked by

—

## Comments

- Filed from a code read of the shipped library (bug hunt, 2026-07-17). This is
  a **documented limitation note**, not a defect against the store contract —
  raised so the tradeoff (and its config implication) is tracked rather than
  tribal knowledge.
- 2026-11-20 — Triage: **wontfix**, as the documented best-effort trade-off.
  The two other candidate responses are already in place. README §6.3
  ("`lock-ttl` must outlast your slowest handler") documents the guidance, and
  commit `f9cd4f3` added the `WARN` on a no-op `complete()`. Keeping a slow but
  live primary's reservation would need a lease-renewal heartbeat, which is
  out of proportion for a store documented as best-effort. Use Postgres when
  that matters. While re-checking, the README turned out to say this scenario
  surfaces as `response_unavailable`. It doesn't: the dropped record means a
  retry is a fresh reservation and **re-runs the handler**. §6.3, the
  `response_unavailable` metric row and §7.3 were corrected to say so, and to
  point at the `WARN` log as the only signal. The repro stays `@Disabled` as
  documentation of the limitation.
- 2026-11-20 — `RedisSlowPrimaryBestEffortTest` is no longer `@Disabled`. It now pins the current behavior (the slow primary's response is dropped, `find()` is empty, and a retry reserves the key afresh), so the limit stays visible in every build.
