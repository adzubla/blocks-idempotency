# Concurrency model: REJECT by default, WAIT via polling, no self-promotion

## Status

accepted

## Context and decision

When two requests with the same effective key arrive concurrently, the first one
**reserves** the key (`in-progress` state) before processing the effect. The second
(the *waiter*) finds the key reserved and needs a defined behavior.

We decided:

- **`REJECT` is the default** — the waiter gets an immediate **409**, without
  holding a thread. `WAIT` is optional per endpoint (`whenInProgress=WAIT`).
- In **`WAIT`** mode, the default **polling model** has the waiter **poll** the key
  state (~100ms with jitter) in the `core` engine — using only the `get` of the
  `IdempotencyStore` interface, so it works on Redis and any store without native
  blocking. Stores with native concurrency control (**Postgres**) instead realize
  `REJECT`/`WAIT` inside their reserve operation (block on the unique index, bounded
  by `lock_timeout`) — see the store-divergence note below.
- In the polling model the waiter **never executes the effect** (no
  *self-promotion*). Outcomes:

  | Primary | Waiter observes | Response |
  |---|---|---|
  | Completed 2xx | key `completed` | cache replay + `Idempotency-Replayed: true` |
  | Failed (non-2xx) | key gone (Q7: error releases the key) | 409 + `Retry-After` |
  | Crashed (lock-ttl expired) | key gone | 409 + `Retry-After` |
  | Still running at `wait-timeout` | still `in-progress` | 409 + `Retry-After` |

- **Status taxonomy**: `2xx` result/replay; **409 + Retry-After** for everything
  retryable (REJECT, WAIT-timeout, key gone), with the reason in a header
  (`in_progress|released|timeout`); **409** *without* `Retry-After` (reason
  `response_unavailable`) when the effect completed but its response can't be
  replayed (crash window or body over `max-body-size`) — terminal, no re-execution;
  **422** fingerprint collision (terminal, reason `collision`); **400** missing
  key with `keyRequired=true` (reason `key_required`) or a key value outside the
  configured size/charset (reason `key_invalid`); **503** fail-closed with no
  store (reason `store_unavailable`). Every one of these non-2xx statuses
  carries its reason in the same `Idempotency-Reject-Reason` header — see the
  resolved open question below.
- **Store divergence (accepted).** The outcomes table is the **polling (Redis)**
  model. **Postgres** matches it *except on primary failure/crash*: the waiter,
  blocked on the reservation `INSERT`, sees the primary roll back, its `INSERT` then
  succeeds, and it **becomes the executor** — native, safe self-promotion (the
  failed primary's effect rolled back, so re-doing it is correct). We accept this
  rather than suppress the database's free, correct behavior; *no self-promotion* is
  thus a property of the polling model, not a universal invariant.

## Considered Options

- **`WAIT` as the default** (rejected) — better UX, but holds the servlet thread;
  under a duplicate storm it exhausts the pool. REJECT is the safe default.
- **Push-based detection (Redis Pub/Sub or BLPOP)** (rejected for v1) — low latency,
  but Redis-specific, breaks the interface's uniformity, and holds a Redis
  connection per waiter. Kept as a v2 optimization via an overridable `await()`,
  with polling as the default.
- **Waiter self-promotion in the polling (Redis) model** when the key is gone
  (rejected for v1) — better UX, but mixes waiting with executing, complicates
  responsibility for the effect, and opens chaining. A future evolution if UX
  demands it. (Postgres already gets safe self-promotion for free via native
  locking — see the store-divergence note.)
- **425 Too Early** instead of 409 (rejected) — marginally more precise semantics on
  timeout, but weak support in proxies/clients and it introduces a second "retry"
  code. A single 409 keeps the client's retry logic identical.

## Consequences

- The public contract depends on **409 + Retry-After** as the "resend with the same
  key" signal; changing that later breaks clients.
- `WAIT` remains the thread-holding mode — `wait-timeout` should be short and,
  ideally, the number of concurrent waiters bounded.
- `await()` as an overridable interface method leaves the door open for per-store
  push without changing the `core`.

## Resolved questions

- **Reason header uniformity across the status taxonomy (resolved).** Originally
  the "reason in a header" decision above was implemented only for **409**
  (`Reject` carrying `Idempotency-Reject-Reason: in_progress|...`); **400**
  (missing required key, Slice 005; later joined by a second cause, a key value
  outside the configured size/charset) and **422** (fingerprint collision, Slice
  002) returned status-only, no reason header — a client couldn't distinguish,
  say, "idempotency key required" from any other 400 the app itself returns, nor
  either 400 cause from each other. **Decided**: extend the
  `Idempotency-Reject-Reason` header to every non-2xx idempotency response, not
  just REJECT — one closed vocabulary (`RejectReason`) shared by all of them:
  `in_progress` / `released` / `timeout` / `response_unavailable` (409),
  `collision` (422), `key_required` / `key_invalid` (400), `store_unavailable`
  (503). Each `IdempotencyException` subtype now carries its own fixed reason
  (`IdempotencyException#reason()`); `IdempotencyExceptionHandler` writes it
  uniformly for every subtype, adding `Retry-After` only for
  `IdempotencyConflictException`, since that's the only retryable-with-the-same-key
  case. A single generic `@ExceptionHandler(IdempotencyException.class)` replaced
  the six per-type handlers, since the response shape is now identical modulo
  that one extra header.
