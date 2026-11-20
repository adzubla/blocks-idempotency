# Slice 067 — Unauthenticated routes share one key space (existence oracle for body-field keys)

> Source: system analysis (2026-11-19) · Type: AFK
> Status: needs-triage

## What to build

By design (CONTEXT.md, Key scope), routes without authentication fall back to
`principal = ""` (`EffectiveKey.NO_PRINCIPAL`), so every anonymous caller shares
one key space per endpoint. The same applies everywhere when
`scope.principal-enabled=false`.

With the header strategy and ULID keys that's harmless, because the keys can't
be guessed. With the **body-field strategy** the key is a business identifier
(`$.order.id`), often sequential or guessable. Any anonymous caller can probe it:

- 201 → the ID has never been submitted
- 422 → the ID **has** been submitted (with a different body)
- 409 → a submission for the ID is in progress right now
- replay → identical body submitted before (the original response is returned)

That's an existence oracle over another party's business identifiers, and a
replay can hand back the original caller's response.

This is a design trade-off, not a code bug. Filed so it's a recorded decision
rather than something only remembered informally.

Candidate responses, for triage:
- Document the risk in the README's body-field strategy section (recommend it
  only on authenticated routes, or with non-guessable identifiers).
- Optionally warn at startup when a `fieldPath` endpoint is configured with
  `principal-enabled=false`.
- Or `wontfix` with rationale.

## Acceptance criteria

- [ ] Triage decision recorded (document / startup warning / `wontfix`).

## Blocked by

—
