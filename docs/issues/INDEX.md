# Issues

## Batch 1 — in-memory store + header strategy

Tracer-bullet slices for the first batch from the PRD build order: `IdempotencyEngine`
+ in-memory fake `IdempotencyStore` + **header strategy**, proven end-to-end before
wiring the real Redis/Postgres bodies. Body-field strategy, Redis, and Postgres were
**out of scope** for this batch. All slices are **AFK**. All implemented.

| # | Slice | Blocked by |
|---|-------|-----------|
| [001](001-header-strategy-happy-path.md) | Header-strategy happy-path replay (foundation) | — |
| [002](002-payload-collision-422.md) | Payload collision → 422 | 001 |
| [003](003-concurrency-409-reject.md) | In-flight concurrency → 409 REJECT | 001 |
| [004](004-error-releases-key.md) | Error releases the key | 001 |
| [005](005-required-key-400.md) | Required key → 400 / `keyRequired` override | 001 |
| [006](006-policy-resolution-inheritance.md) | Policy resolution + global-default inheritance | 001 |
| [007](007-store-failure-posture.md) | Store-failure posture (fail-open / fail-closed) | 001, 006 |
| [008](008-expiration-new-key.md) | Expiration = new key | 001, 006 |
| [009](009-wait-mode-polling.md) | WAIT mode (core polling) + `response_unavailable` | 001, 003 |
| [010](010-startup-validation.md) | Startup validation (fail-fast) | 001 |
| [011](011-metrics.md) | Metrics | 001 |

## Batch 2 — body-field strategy

The second key resolution strategy (PRD build-order step 4). Most of the mechanics
(engine, policy, fingerprint/collision, `keyRequired`) already exist and are
strategy-agnostic — this batch is scoped to resolution + wiring, not re-plumbing.
Redis and Postgres remain out of scope. All slices are **AFK**.

| # | Slice | Blocked by |
|---|-------|-----------|
| [012](012-body-field-strategy-happy-path.md) | Body-field strategy: happy-path replay | 001 |
| [013](013-body-field-strategy-edge-cases.md) | Body-field strategy: missing/malformed field, non-scalar values, collision reuse | 012 |

## Batch 3 — Redis and Postgres stores

The two real `IdempotencyStore` implementations (currently scaffolds whose
methods all throw `UnsupportedOperationException`), plus the shared contract
test suite the PRD calls for. The Postgres batch had a HITL slice up front: the
current filter+interceptor architecture didn't yet make good on ADR 0001's
"reservation runs inside the effect's transaction" promise. Resolved by
[ADR 0003](../adr/0003-postgres-transaction-participation.md) — the store owns
its own transaction (opened in `reserve()`, closed in `complete()`/`release()`),
not the interceptor.

| # | Slice | Type | Blocked by |
|---|-------|------|-----------|
| [014](014-store-contract-test-suite.md) | `IdempotencyStore` contract test suite | AFK | — |
| [015](015-redis-store-end-to-end.md) | Redis store: end-to-end (happy path, outage, WAIT) | AFK | 014 |
| [016](016-postgres-transaction-participation-decision.md) | HITL: how does the Postgres reservation join the effect's own transaction? | HITL | — |
| [017](017-postgres-store-reserve-complete-release.md) | Postgres store: reserve/complete/release | AFK | 014 |
| [018](018-postgres-native-concurrency-and-cleanup.md) | Postgres store: native concurrency, safe self-promotion, expiration cleanup | AFK | 017 |
| [019](019-postgres-native-await.md) | `await()` as a real `IdempotencyStore` operation, with a native Postgres override | AFK | 009, 018 |

Not published to an issue tracker — none is configured in this workspace. To turn
these into tracked, agent-ready issues, set up the tracker and re-run the breakdown
against these files (apply the `ready-for-agent` triage label; `ready-for-human`
for the HITL slice).

## Batch 4 — bug hunt (2026-07-17)

Defects found by a code read of the shipped library; each filed with a
`@Disabled` repro test, unfixed by request (expose now, fix later). Removing a
test's `@Disabled` reproduces its failure. All slices are **AFK**.

| # | Slice | Type | Blocked by |
|---|-------|------|-----------|
| [028](028-fingerprint-float-precision-collision.md) | Fingerprint collapses distinct float payloads → missed collision (replays wrong response) | AFK | — |
| [029](029-store-reserve-conflict-vanished-record-500.md) | `reserve()` re-read race throws unmapped exception → raw 500, bypasses `onStoreFailure` | AFK | — |
| [030](030-wait-mode-interrupt-uncontrolled-500.md) | Interrupt during WAIT polling escapes as `IllegalStateException` → 500 | AFK | — |
| [031](031-default-await-clock-and-initial-check.md) | Default `await()` uses wall-clock (ignores injected `Clock`) and sleeps before first `find()` | AFK | — |
| [032](032-redis-slow-primary-loses-response.md) | Redis silently drops a slow primary's response once `lock-ttl` passes (documented best-effort limitation) | AFK | — |
| [033](033-postgres-dangling-reservation-scope.md) | Harden against a dangling Postgres reservation scope surviving thread reuse (defense-in-depth) | AFK | — |

## Batch 5 — messaging extension: core refactor + Kafka (2026-07-20)

Tracer-bullet slices from `docs/prd/messaging-extension.md`'s Phase 1/Phase 2
build order, sharpened via a `/grill-with-docs` session — see
[ADR 0004](../adr/0004-messaging-dedupe-only-v1-scope.md) (dedupe-only v1
scope) and [ADR 0005](../adr/0005-messaging-wait-disabled.md) (`WAIT`
disabled for messaging). RabbitMQ and JMS (PRD Phase 3) are **out of scope**
for this batch. All slices are **AFK**.

| # | Slice | Blocked by |
|---|-------|-----------|
| [034](034-messaging-core-route-handler-refactor.md) | Core refactor: route/handler generalization + messaging-ready primitives | — |
| [035](035-kafka-happy-path-dedupe-skip.md) | Kafka happy-path dedupe-skip (foundation) | 034 |
| [036](036-kafka-collision-dead-letter.md) | Kafka collision → dead-letter | 035 |
| [037](037-kafka-concurrent-duplicate-ack-skip.md) | Kafka concurrent in-progress duplicate → ack-and-skip | 035 |
| [038](038-kafka-store-failure-posture.md) | Kafka store-failure posture → nack-with-backoff / proceed unprotected | 035 |
| [039](039-kafka-missing-invalid-key-dead-letter.md) | Kafka missing/invalid key → dead-letter | 035 |
| [040](040-kafka-startup-validation-wait-disabled.md) | Kafka startup validation: reject `whenInProgress=WAIT` | 035 |
| [041](041-kafka-redis-store-end-to-end.md) | Redis store wired into the Kafka module end-to-end | 035 |
| [042](042-kafka-postgres-store-end-to-end.md) | Postgres store wired into the Kafka module end-to-end | 035, 034 |
