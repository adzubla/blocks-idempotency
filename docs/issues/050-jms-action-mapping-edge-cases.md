# Slice 050 — JMS action-mapping edge cases

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The remaining rows of the PRD §5 action-mapping table, against the JMS
module built in slice 049, using `messaging-core`'s shared action-mapping
translation (slice 048) rather than re-deriving it:

- **Collision** (`COLLISION`) → **dead-letter**, via JMS's own dead-letter
  mechanism (broker-specific DLQ, or `JmsListenerErrorHandler` routing to one).
- **Concurrent in-progress duplicate** (`IN_PROGRESS`/`REJECT`) →
  **ack-and-skip**.
- **Store-failure posture** (`STORE_UNAVAILABLE`) → **nack-with-backoff**
  (`onStoreFailure=CLOSED`) or **proceed unprotected** (`OPEN`) — for JMS,
  nack-with-backoff means rolling back the session/rejecting redelivery
  count-limited, relying on the broker's own redelivery/DLQ policy.
- **Missing/invalid key** (`KEY_REQUIRED`/`KEY_INVALID`) → **dead-letter**.

## Acceptance criteria

- [ ] `COLLISION` → dead-lettered, not redelivered as-is
- [ ] Concurrent in-progress duplicate → ack-and-skip, listener not
      re-invoked
- [ ] `STORE_UNAVAILABLE` → nack-with-backoff (`onStoreFailure=CLOSED`) or
      proceed unprotected (`onStoreFailure=OPEN`)
- [ ] `KEY_REQUIRED`/`KEY_INVALID` → dead-lettered
- [ ] Each outcome covered by an integration test against the JMS module
      from slice 049, reusing `messaging-core`'s shared mapping logic rather
      than a JMS-local reimplementation

## Blocked by

049
