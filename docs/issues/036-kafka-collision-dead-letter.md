# Slice 036 — Kafka collision → dead-letter

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A duplicate delivery with the same key but a different body fingerprint —
HTTP's 422 collision case — is routed to the configured dead-letter topic
instead of invoking the listener, per the PRD §5 action-mapping table. A key
reused with a different payload on a message broker is a producer bug or
poison message; no consumer-side retry resolves it, so this is terminal.

## Acceptance criteria

- [ ] A delivery with the same key but a different body fingerprint is
      routed to the dead-letter topic
- [ ] The listener is **not** invoked on a collision
- [ ] The original (first) delivery's completion is unaffected
- [ ] Covered by an embedded-Kafka end-to-end test

## Blocked by

035
