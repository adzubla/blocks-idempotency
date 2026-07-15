# Slice 001 — Header-strategy happy-path replay (foundation)

> Source: docs/prd/idempotency-library.md · Type: AFK

## What to build

The end-to-end spine of the library, using the **header strategy** and an
**in-memory `IdempotencyStore`** (which becomes the reusable test double for every
later slice).

A handler annotated `@Idempotent(header="X-Idempotency-Key")` is intercepted
end-to-end: a servlet filter buffers the request/response, a handler interceptor
reads the annotation and drives the `IdempotencyEngine`. The engine resolves the
effective key (endpoint + principal + header value; principal sentinel on
unauthenticated routes), reserves it in the store, proceeds to the handler,
captures the 2xx response (status, headers minus the denylist — `Set-Cookie`
always removed — and body), completes the record, and on a repeat with the same
key replays the stored response verbatim with `Idempotency-Replayed: true`.

Header strategy only. No fingerprint/collision, concurrency, or policy inheritance
yet — those are later slices.

## Acceptance criteria

- [x] `@Idempotent(header=…)` on an MVC handler is intercepted end-to-end.
- [x] First request executes the handler and caches the 2xx response.
- [x] A repeat with the same key returns the cached status, headers, and body
      **without re-executing** the handler (the effect runs exactly once).
- [x] Replayed responses carry `Idempotency-Replayed: true`.
- [x] `Set-Cookie` is never replayed.
- [x] The effective key is scoped by method + path + principal + header value.
- [x] An in-memory `IdempotencyStore` implements the SPI
      (reserve / find / complete / release) as a test double.
- [x] Covered by `IdempotencyEngine` decision-tree tests (happy path) and a MockMvc
      end-to-end test.

## Blocked by

None - can start immediately.
