# Slice 041 — Redis store wired into the Kafka module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The mechanics proven in slices 035–039 (happy-path dedupe-skip, collision,
concurrent duplicate, store-failure posture) re-verified against the real
`RedisIdempotencyStore` instead of the in-memory test double, using
Testcontainers. No store-level code changes are expected — this verifies the
PRD's claim that Redis needs no changes for messaging, since it already keys
opaquely off `EffectiveKey.digestBytes()`.

## Acceptance criteria

- [x] Happy-path dedupe-skip works end-to-end against a real Redis instance
- [x] Collision, concurrent-duplicate, and store-failure behaviors are
      re-verified against Redis
- [x] No changes needed to `RedisIdempotencyStore` itself — if something
      does surface, it's captured as a new finding, not silently patched
      around

## Blocked by

035
