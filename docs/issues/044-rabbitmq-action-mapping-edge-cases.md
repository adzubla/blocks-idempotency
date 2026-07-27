# Slice 044 — RabbitMQ action-mapping edge cases

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The remaining rows of the PRD §5 action-mapping table, against the RabbitMQ
module built in slice 043 — mirroring what the Kafka module proved across
its slices 036–039, merged here into one slice:

- **Collision** (fingerprint mismatch, `COLLISION`) → **dead-letter**. A key
  reused with a different payload is a producer bug or poison message.
- **Concurrent in-progress duplicate** (`IN_PROGRESS`/`REJECT`) →
  **ack-and-skip**. The other in-flight delivery already owns the effect;
  nacking would just cause a redundant redelivery loop.
- **Store-failure posture** (`STORE_UNAVAILABLE`) → **nack-with-backoff**
  under `onStoreFailure=CLOSED`, **proceed unprotected** under `OPEN`.
  Transient infra trouble, not a poison message.
- **Missing/invalid key** (`KEY_REQUIRED`/`KEY_INVALID`) → **dead-letter**. A
  structurally bad message that broker-native redelivery can't fix.

All four map onto RabbitMQ's own primitives: dead-letter via a configured
dead-letter exchange/queue, nack-with-backoff via `channel.basicNack`
(requeue=false, relying on a retry/backoff policy — e.g. a DLX with TTL, or
`RetryOperationsInterceptor`), ack-and-skip via `channel.basicAck` without
invoking the listener body.

## Acceptance criteria

- [x] `COLLISION` → dead-lettered, not redelivered as-is
- [x] Concurrent in-progress duplicate → ack-and-skip, listener not
      re-invoked
- [x] `STORE_UNAVAILABLE` → nack-with-backoff (`onStoreFailure=CLOSED`) or
      proceed unprotected (`onStoreFailure=OPEN`), matching the `IdempotencyEngineRegistry`
      policy the same way Kafka's slice 038 did
- [x] `KEY_REQUIRED`/`KEY_INVALID` → dead-lettered
- [x] Each outcome covered by an integration test against the RabbitMQ
      module from slice 043

## Blocked by

043
