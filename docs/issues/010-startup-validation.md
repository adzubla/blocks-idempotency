# Slice 010 — Startup validation (fail-fast)

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

At application startup, scan `@Idempotent` handlers and **fail fast** with a clear
message if: not exactly one of `header` / `fieldPath` is set; `ttl` (when non-empty)
is not a valid `Duration`; or the referenced `store` qualifier has no matching bean
on the classpath (module not included).

## Acceptance criteria

- [x] Both or neither of `header` / `fieldPath` → startup fails with a clear message.
- [x] An invalid `ttl` string → startup fails.
- [x] A `store` qualifier with no matching bean → startup fails, naming the missing
      module.
- [x] Valid configurations start normally.
- [x] Tests assert the context fails to start for each misconfiguration.

## Blocked by

- Slice 001 — header-strategy happy-path replay
