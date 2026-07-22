# Slice 052 — Redis store wired into the JMS module end-to-end

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The mechanics proven in slices 049–051 (happy-path dedupe-skip, action-mapping
edge cases, WAIT-rejection) re-verified against the real
`RedisIdempotencyStore` instead of the in-memory test double, using
Testcontainers — mirroring what the Kafka and RabbitMQ modules already
proved. No store-level code changes are expected.

## Acceptance criteria

- [ ] Happy-path dedupe-skip works end-to-end against a real Redis instance
- [ ] Collision, concurrent-duplicate, and store-failure behaviors are
      re-verified against Redis
- [ ] No changes needed to `RedisIdempotencyStore` itself — if something
      does surface, it's captured as a new finding, not silently patched
      around

## Blocked by

049
