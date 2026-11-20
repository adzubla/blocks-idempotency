# Slice 063 — Kafka/JMS: a deserialized payload is fingerprinted as an empty body

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

`KafkaIdempotencyAdvice.KafkaMessageDelivery.body()` only handles `byte[]` and
`String` record values. Anything else, such as a POJO from `JsonDeserializer`
or an Avro record, returns `new byte[0]`. `JmsIdempotencyAdvice` does the same
for any message type other than `TextMessage` or `BytesMessage`
(`ObjectMessage`, `MapMessage`).

Consequences, silent at runtime and not checked at startup:
- **`fieldPath` strategy:** `BodyFieldKeyStrategy.resolve` gets an empty body,
  so it never finds a key. With the default `keyRequired=true`, **every message
  is sent to the dead-letter destination** as "key missing".
- **`header` strategy:** every fingerprint hashes the same empty body, so
  **collision detection is silently disabled**. A reused key with a different
  payload is ack-and-skipped as a duplicate instead of dead-lettered.

The README doesn't mention a requirement on the value type.

Candidate fixes, for triage:
- Fingerprint the raw bytes before deserialization. Kafka exposes them through
  an `ErrorHandlingDeserializer`/`RecordInterceptor`-style hook, or the
  listener can consume `ConsumerRecord<?, byte[]>`.
- Or fail fast: reject a non-raw payload at first delivery with a clear error,
  or document the requirement and warn once at runtime.

## Acceptance criteria

- [ ] A `fieldPath` listener on a deserialized-POJO topic either resolves its
      key or fails loudly with a clear explanation, never by silently
      dead-lettering every message.
- [ ] A header-strategy listener detects a payload collision regardless of value
      type, or the limitation is documented and warned about.

## Blocked by

—
