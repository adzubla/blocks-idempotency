# Slice 006 — Policy resolution + global-default inheritance

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

A `PolicyResolver` that computes the effective per-request policy from `@Idempotent`
attributes plus the global `idempotency.*` defaults, honoring the inheritance
sentinels: `store=""` and `ttl=""` inherit; `onStoreFailure=DEFAULT` and
`whenInProgress=DEFAULT` inherit; `keyRequired` is a fixed boolean that **never**
inherits. Refactors Slice 001's inline defaults to flow through the resolver.

## Acceptance criteria

- [x] Attributes left at their sentinel resolve to the configured global default.
- [x] Explicit attributes override the global default (precedence: endpoint > global).
- [x] `keyRequired` ignores any global config (fixed per-endpoint boolean).
- [x] Changing a global default (e.g. `default-ttl`, `default-when-in-progress`)
      changes effective behavior with no code change.
- [x] Unit tests for the resolver across sentinel/override combinations.

## Blocked by

- Slice 001 — header-strategy happy-path replay
