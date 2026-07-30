# Slice 057 — Redis store wired into the SQS module end-to-end

> Source: docs/prd/messaging-extension.md (extends the PRD's JMS/RabbitMQ/Kafka scope to SQS — see Batch 8 note in INDEX.md) · Type: AFK
> Status: ready-for-agent

## What to build

The mechanics proven in slices 054–056 (happy-path dedupe-skip,
action-mapping edge cases, WAIT-rejection) re-verified against the real
`RedisIdempotencyStore` instead of the in-memory test double, using
Testcontainers — mirroring what the Kafka (041), RabbitMQ (046), and JMS
(052) modules each proved. No store-level code changes are expected — this
reconfirms the PRD's claim that Redis needs no changes for messaging, since
it already keys opaquely off `EffectiveKey.digestBytes()`.

## Acceptance criteria

- [ ] Happy-path dedupe-skip works end-to-end against a real Redis instance
- [ ] Collision, concurrent-duplicate, and store-failure behaviors are
      re-verified against Redis
- [ ] No changes needed to `RedisIdempotencyStore` itself — if something
      does surface, it's captured as a new finding, not silently patched
      around

## Blocked by

054
