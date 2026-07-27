# Slice 046 — Redis store wired into the RabbitMQ module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The mechanics proven in slices 043–045 (happy-path dedupe-skip, action-mapping
edge cases, WAIT-rejection) re-verified against the real
`RedisIdempotencyStore` instead of the in-memory test double, using
Testcontainers — mirroring what the Kafka module's slice 041 proved. No
store-level code changes are expected — this reconfirms the PRD's claim that
Redis needs no changes for messaging, since it already keys opaquely off
`EffectiveKey.digestBytes()`.

## Acceptance criteria

- [x] Happy-path dedupe-skip works end-to-end against a real Redis instance
- [x] Collision, concurrent-duplicate, and store-failure behaviors are
      re-verified against Redis
- [x] No changes needed to `RedisIdempotencyStore` itself — if something
      does surface, it's captured as a new finding, not silently patched
      around

## Blocked by

043
