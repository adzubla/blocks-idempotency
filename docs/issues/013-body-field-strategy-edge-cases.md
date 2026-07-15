# Slice 013 — Body-field strategy: missing/malformed field, non-scalar values, collision reuse

> Source: docs/prd/idempotency-library.md · Type: AFK
> Status: ready-for-agent

## What to build

Prove the mechanisms already built for the header strategy — `keyRequired`
400/pass-through (Slice 005) and fingerprint collision 422 (Slice 002) —
generalize correctly to the body-field strategy without new production code,
plus define the one genuinely new edge case this strategy introduces: a
non-scalar JSONPath match.

A missing field, a non-JSON request body, or a JSONPath match that resolves to
a JSON **object or array** (e.g. `fieldPath="$.order"` instead of
`"$.order.id"`) are all treated as **no key found** — a key must be a scalar
identity, not a compound structure — falling back to the existing
`keyRequired` branch (400 if required, unprotected pass-through if not) with
no new error path and no crash.

## Acceptance criteria

- [x] Missing field with `keyRequired=true` (default) → 400, handler not
      executed.
- [x] Missing field with `keyRequired=false` → handler executes unprotected, no
      record created.
- [x] A non-JSON request body on a `fieldPath`-configured endpoint is treated as
      no key found (same `keyRequired` branching), not a 500.
- [x] A JSONPath match that is a JSON object or array is treated as no key
      found (same `keyRequired` branching).
- [x] Same field value + a different request body → 422 collision (reusing the
      existing fingerprint mechanism).
- [x] `BodyFieldKeyStrategy` unit tests cover: missing field, malformed JSON,
      object/array match.
- [x] A MockMvc end-to-end test covers at least one of the 400 and 422 paths for
      the body-field strategy.

## Blocked by

- Slice 012 — body-field strategy: happy-path replay
