# whenInProgress=WAIT disabled for messaging listeners

## Status

accepted

## Context and decision

`whenInProgress=WAIT` (ADR 0002) blocks the calling thread in
`store.await()` until an in-flight duplicate resolves, then replays or
rejects based on the outcome. For HTTP this trades a servlet thread for
better client UX than an immediate 409. Extending `@Idempotent` to message
listeners (`docs/prd/messaging-extension.md`) raised whether the same option
should be available there.

We decided **`WAIT` is rejected at startup validation** for `@Idempotent` on
a message listener method — each transport's own listener validator (Kafka's
`KafkaIdempotentListenerValidator`, RabbitMQ's
`RabbitIdempotentListenerValidator`) adds this rule alongside the existing
key-strategy/ttl/store checks (`IdempotentHandlerValidator`'s HTTP
equivalent), rather than silently coercing `WAIT` to `REJECT`. Only
`whenInProgress=REJECT` is supported for v1 messaging, across every
transport.

Reasoning:

- **Blocking a listener container thread is a worse failure mode than
  blocking an HTTP request thread, for reasons specific to each transport.**
  A blocked servlet thread just costs pool capacity — a bounded,
  well-understood degradation. A blocked Kafka listener thread risks missing
  `max.poll.interval.ms`, which the broker treats as a dead consumer and
  triggers a **partition rebalance** — a much more disruptive failure than
  thread-pool pressure, and one most teams won't know to tune around (it
  depends on the interaction between `wait-timeout`, `lock-ttl`, and the
  consumer's poll-interval configuration). A blocked RabbitMQ listener
  thread risks the container treating the consumer as stalled - failing
  differently in detail, but landing in the same place: a much more
  disruptive failure than the thread-pool pressure HTTP tolerates.
- **WAIT is arguably redundant for messaging specifically.** For HTTP, if
  the primary fails, nothing retries automatically — that's *why* the
  library replies 409+Retry-After and relies on the client to resend. For
  messaging, if the primary's delivery fails before it acks/commits, the
  broker itself redelivers that same message natively (at-least-once
  delivery) — Kafka via offset redelivery, RabbitMQ via broker-native
  requeue. "Wait and see if the primary succeeds" duplicates something the
  transport already does for the primary's own message.

## Considered Options

- **Clamp `wait-timeout` below a safe fraction of `max.poll.interval.ms`**
  (rejected for v1) — a middle path keeping WAIT available but validated
  against the rebalance risk instead of documentation-only. Rejected because
  it doesn't remove the actual complexity: the reject-path action mapping
  still needs a 3-way split (`IN_PROGRESS`→ack-and-skip vs.
  `RELEASED`/`TIMEOUT`→nack-with-backoff, instead of REJECT-only's single
  ack-and-skip case), and hard-to-write, rebalance-simulating embedded-Kafka
  integration tests are still needed to prove the clamp actually holds under
  real listener-container behavior. Revisit if a concrete need for WAIT's
  correctness fallback (see Consequences) outweighs this cost.

## Consequences

- **Loses a narrow correctness fallback.** WAIT lets a second delivery
  observe `RELEASED`/`TIMEOUT` and choose nack-with-backoff instead of
  trusting the primary blindly through the crash window between "effect
  ran" and "ack recorded." Under REJECT-only, that duplicate is always
  ack-and-skipped, betting entirely on the primary's own broker-native
  redelivery to cover that window. This mirrors the same crash-window risk
  `lock-ttl` already tolerates for HTTP, but messaging has no second
  delivery left over to fall back on once it's been acked away.
- **Transport-conditional exception to `CONTEXT.md`'s "single mechanism,
  same policy" principle.** The same `@Idempotent` attribute
  (`whenInProgress`) becomes invalid depending on what kind of method it
  decorates — the messaging handler validator enforces a rule the HTTP
  validator doesn't have.
- `RELEASED`/`TIMEOUT` never appear in the messaging action-mapping table
  (`docs/prd/messaging-extension.md` §5) as a result — only relevant again
  if this decision is revisited.
