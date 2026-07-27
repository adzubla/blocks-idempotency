# Slice 037 — Kafka concurrent in-progress duplicate → ack-and-skip

> Source: docs/prd/messaging-extension.md · Type: AFK
> Status: ready-for-agent

## What to build

A second, concurrent delivery of the same key (e.g. two partitions/consumers
racing, or a rebalance re-delivering mid-processing) finds the key already
reserved and is acked-and-skipped without re-invoking the listener — the
PRD §5 action mapping for `IN_PROGRESS` under the REJECT-only default
(`whenInProgress=WAIT` is out of scope for v1 — see slice 040 and
`docs/adr/0005-messaging-wait-disabled.md`). Safe because the primary's own
message is natively redelivered by the broker if it fails — the duplicate
isn't needed as a backup.

## Acceptance criteria

- [x] A concurrent duplicate delivery (key already reserved, same
      fingerprint) is acked and the listener is **not** invoked
- [x] The primary's own execution and completion are unaffected by the
      concurrent duplicate
- [x] Covered by a test simulating two concurrent deliveries of the same key

## Blocked by

035
