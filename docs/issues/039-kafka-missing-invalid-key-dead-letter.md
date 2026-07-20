# Slice 039 — Kafka missing/invalid key → dead-letter

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

The bad-key cases, per the PRD §5 action-mapping table:

- `keyRequired=true` (the default) with no resolvable key, or a key value
  outside the configured size/charset — HTTP's 400 cases — is routed to the
  dead-letter topic without invoking the listener. A structurally bad
  message; broker-native redelivery can't fix a message that will never
  carry a valid key.
- `keyRequired=false` with no key present: the listener executes normally,
  unprotected (client opt-in, same as HTTP).

## Acceptance criteria

- [ ] `keyRequired=true` + missing key → dead-letter, listener not invoked
- [ ] `keyRequired=true` + invalid key (size/charset) → dead-letter,
      listener not invoked
- [ ] `keyRequired=false` + missing key → listener executes normally,
      unprotected
- [ ] Covered by tests for each case

## Blocked by

035
