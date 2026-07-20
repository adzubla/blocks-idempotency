# Slice 038 — Kafka store-failure posture → nack-with-backoff / proceed unprotected

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The store-unavailable posture, per the PRD §5 action-mapping table:

- `onStoreFailure=CLOSED` + store unavailable: the message is **not**
  acknowledged and the listener is **not** invoked, so the broker redelivers
  once the store is likely to have recovered. Transient infrastructure
  trouble, not a poison message — dead-letter would be wrong, and
  ack-and-skip would risk silently losing the message since it's unknown
  whether the effect already ran.
- `onStoreFailure=OPEN` (the default) + store unavailable: the listener
  executes normally, unprotected — no idempotency tracking attempted.

## Acceptance criteria

- [ ] `onStoreFailure=CLOSED` + store unavailable → message is not acked,
      listener is not invoked
- [ ] `onStoreFailure=OPEN` + store unavailable → listener executes
      normally, unprotected
- [ ] Covered by tests simulating store unavailability under both postures

## Blocked by

035
