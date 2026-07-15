# Slice 011 — Metrics

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

Emit Micrometer counters for the key outcomes: **replay**, **422
collision**, **409 concurrency**, and **fail-open activation**. Toggle via
`idempotency.metrics.enabled`.

## Acceptance criteria

- [x] Counters increment for replay, collision (422), concurrency (409), and fail-open.
- [x] Metrics can be disabled via configuration.
- [x] Tests assert counter increments through the engine.

## Blocked by

- Slice 001 — header-strategy happy-path replay
