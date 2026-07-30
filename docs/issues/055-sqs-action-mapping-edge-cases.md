# Slice 055 — SQS action-mapping edge cases

> Source: docs/prd/messaging-extension.md (extends the PRD's JMS/RabbitMQ/Kafka scope to SQS — see Batch 8 note in INDEX.md) · Type: AFK
> Status: ready-for-agent

## What to build

The remaining rows of the PRD §5 action-mapping table, against the SQS
module built in slice 054 — mirroring what the Kafka (036–039), RabbitMQ
(044), and JMS (050) modules each proved:

- **Collision** (fingerprint mismatch, `COLLISION`) → **dead-letter**. A key
  reused with a different payload is a producer bug or poison message.
- **Concurrent in-progress duplicate** (`IN_PROGRESS`/`REJECT`) →
  **ack-and-skip**. The other in-flight delivery already owns the effect;
  leaving the message unacked would just cause a redundant redelivery once
  its visibility timeout expires.
- **Store-failure posture** (`STORE_UNAVAILABLE`) → **nack-with-backoff**
  under `onStoreFailure=CLOSED`, **proceed unprotected** under `OPEN`.
  Transient infra trouble, not a poison message.
- **Missing/invalid key** (`KEY_REQUIRED`/`KEY_INVALID`) → **dead-letter**. A
  structurally bad message that redelivery can't fix.

Unlike RabbitMQ, plain SQS has no consumer-triggered "reject straight to
DLQ" primitive — dead-lettering is a queue-level redrive policy
(`maxReceiveCount` on the source queue's redrive configuration), which only
fires after N deliveries, not on a single consumer decision. So, like the
JMS module's app-managed `JmsDeadLetterPublisher`, this slice adds an
`SqsDeadLetterPublisher` that explicitly sends the message to its
configured dead-letter queue (via `SqsTemplate`/`SqsAsyncClient`) and then
deletes the original from the source queue (so it isn't left to accumulate
toward the redrive policy's own count) — dead-lettered on the first bad
delivery, not the Nth. **Store-failure** (`CLOSED`) instead leaves the
message neither deleted nor acknowledged, relying on its own visibility
timeout expiring so SQS makes it visible again for redelivery once the
store has likely recovered — the SQS equivalent of JMS's "leave un-acked"
and Kafka's "nack-with-backoff". **Ack-and-skip** deletes the message
without invoking the listener body.

## Acceptance criteria

- [ ] `COLLISION` → routed to the configured dead-letter queue via
      `SqsDeadLetterPublisher`, then deleted from the source queue
- [ ] Concurrent in-progress duplicate → ack-and-skip (deleted without
      invoking the listener)
- [ ] `STORE_UNAVAILABLE` → left un-acked for visibility-timeout-driven
      redelivery (`onStoreFailure=CLOSED`) or proceed unprotected
      (`onStoreFailure=OPEN`), matching the `IdempotencyEngineRegistry`
      policy the same way Kafka's slice 038 and JMS's slice 050 did
- [ ] `KEY_REQUIRED`/`KEY_INVALID` → routed to the dead-letter queue the
      same way as `COLLISION`
- [ ] Each outcome covered by an integration test against the SQS module
      from slice 054 (LocalStack)

## Blocked by

054
