# Slice 068 — Messaging duplicates are counted as `response_unavailable`

> Source: documentation review (2026-11-20) · Type: AFK
> Status: needs-triage

## What to build

The `idempotency.outcomes` counter reports the wrong outcome for message
listeners. Messaging is dedupe-only (ADR 0004): the advice completes every record
with `CachedResponse.empty()`, so a completed record never has a body. In
`IdempotencyEngine.decisionForCompleted` that takes the "not replayable" branch,
which calls `metrics.recordResponseUnavailable(...)`.

The result:

- Every routine duplicate delivery that is acked and skipped increments
  `outcome=response_unavailable`. `outcome=replay` never fires for messaging.
- For HTTP, `response_unavailable` is an abnormal condition (a response over
  `max-body-size`, or the capture filter missing), and the README tells operators
  to alert on a sustained rate. For messaging the same counter is the normal
  duplicate count, so an alert on it either fires constantly or has to exclude
  listeners by `route`/`handler`.

The engine's own javadoc already notes that for messaging `Unavailable` is "its
routine, expected outcome" and not an error. The metric doesn't follow that
distinction. Today the README documents the quirk ("HTTP: abnormal, messaging:
normal") instead of fixing it.

Candidate fixes, for triage:

- Record a distinct outcome for a completed duplicate that was skipped on
  purpose, for example `duplicate_skipped`. This needs a way to tell the two
  `Unavailable` cases apart: a new `EngineDecision` variant, or a flag on
  `Unavailable`, or the caller telling the engine it never caches a response.
- Keep `response_unavailable` for HTTP only.
- Decide whether to rename for existing dashboards. Adding a new outcome and
  stopping to record `response_unavailable` for messaging changes what existing
  queries return, so note it in the release notes, or ship it before the first
  release while the project is at `0.1.0-SNAPSHOT`.

## Acceptance criteria

- [ ] An acked-and-skipped messaging duplicate no longer increments `response_unavailable`.
- [ ] It increments a distinct outcome that the README documents as normal.
- [ ] An HTTP response that can't be replayed still increments `response_unavailable`.
- [ ] `MicrometerIdempotencyMetrics`, `NoOpIdempotencyMetrics`, the `IdempotencyMetrics` interface and the README outcome table are updated, and the "six counters" wording (properties javadoc, README, `CONTEXT.md`) matches.
- [ ] A test per transport asserts the recorded outcome (engine level for HTTP and messaging).

## Blocked by

—
