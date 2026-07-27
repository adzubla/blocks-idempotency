# Slice 034 — Core refactor: route/handler generalization + messaging-ready primitives

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

Prefactoring for the Kafka work in slices 035+ — no messaging code in this
slice, purely generalizing `core` (and `store-postgres`) so the concepts stop
being HTTP-named. Per the PRD §3 and the phased build order's Phase 1.

- Rename `EffectiveKey`'s record components `method`/`path` → `route`/`handler`
  (`route` = where it came in on — HTTP path, or a future message
  destination/topic; `handler` = which code processes it — HTTP method+
  controller, or a future listener id/consumer group). Update
  `Fingerprint.sha256(...)`'s params to match, and every call site:
  `EffectiveKeyFactory`, `IdempotencyInterceptor`, `RedisIdempotencyStore`,
  `PostgresIdempotencyStore`, and all existing tests. Pure rename — no
  behavior change for HTTP.
- Edit `V1__idempotency_record.sql` directly, renaming the `http_method`/
  `path` columns to `route`/`handler` (the library is unreleased,
  `0.1.0-SNAPSHOT` — no production deployments to migrate through, so no V2
  migration is needed). Update `PostgresIdempotencyStore`'s SQL
  (`UPSERT_RESERVATION`/`SELECT_RECORD`/`COMPLETE`) to match.
- Add a `CachedResponse.empty()` (or equivalent named constant) sentinel
  factory representing "completed, nothing to cache." Add javadoc on
  `IdempotencyEngine.decisionForCompleted()` documenting that a bodyless
  completed response resolves to `EngineDecision.Unavailable` — for HTTP
  that's a terminal error (crash window/oversized body), but a future
  messaging caller completing every record with this sentinel will see
  `Unavailable` as its **routine** duplicate-skip outcome, not an error (see
  `docs/adr/0004-messaging-dedupe-only-v1-scope.md`). Documented now so it
  isn't "rediscovered" as a bug mid-Kafka-build.
- Add a transport-neutral `EffectiveKeyFactory.create(String route, String
  handler, String principal, String value)` overload alongside the existing
  `HttpServletRequest`-based one, so a future messaging caller assembles keys
  without needing its own parallel logic.

## Acceptance criteria

- [x] `EffectiveKey` has `route`/`handler`/`principal`/`value` fields — no
      leftover `method`/`path` references anywhere in `core` or
      `store-postgres`
- [x] `Fingerprint.sha256(route, handler, body)` — param names updated
- [x] `V1__idempotency_record.sql`'s columns are `route`/`handler` (edited
      directly, no V2 migration file); `PostgresIdempotencyStore`'s SQL
      updated to match
- [x] `CachedResponse.empty()` (or equivalent) exists; a test asserts
      `IdempotencyEngine.decisionForCompleted(...)` on a record completed
      with it returns `Unavailable`
- [x] `EffectiveKeyFactory.create(String route, String handler, String
      principal, String value)` overload exists, covered by a unit test
- [x] Full existing test suite passes unchanged — pure rename/addition, no
      behavior change: `mvn test -pl blocks-idempotency-core -am`,
      `mvn test -pl blocks-idempotency-store-postgres -am`,
      `mvn test -pl blocks-idempotency-store-redis -am`

## Blocked by

None - can start immediately.
