# Slice 012 — Body-field strategy: happy-path replay

> Source: docs/prd/idempotency-library.md · Type: AFK
> Status: ready-for-agent

## What to build

The second key resolution strategy (see `CONTEXT.md` — Key Resolution Strategy):
`@Idempotent(fieldPath="$.order.id")` extracts the raw key from a JSONPath into the
**buffered raw request body** (not the controller's deserialized type), instead of a
client-supplied header. A `BodyFieldKeyStrategy` mirrors `HeaderKeyStrategy`'s
contract (`Optional<String>`, absent when the path doesn't resolve), and
`IdempotencyInterceptor` picks whichever strategy matches the annotation's
non-empty attribute (`header`/`fieldPath` are already validated as
exactly-one-set at startup — Slice 010).

Everything downstream of key resolution — the engine, policy resolution,
fingerprinting, response capture/replay — is already strategy-agnostic (it
operates on a raw key string and body bytes regardless of where the key came
from), so this slice is scoped to resolution + wiring, not re-plumbing the
engine.

## Acceptance criteria

- [x] `@Idempotent(fieldPath=...)` on an MVC handler is intercepted end-to-end via
      the body-field strategy.
- [x] First request executes the handler and caches the 2xx response.
- [x] A repeat with the same JSONPath value replays without re-executing the
      handler.
- [x] Two requests whose JSONPath values differ are isolated (both execute,
      separately cached).
- [x] A JSONPath match that is a JSON number or boolean is coerced to its string
      form as the raw key.
- [x] `BodyFieldKeyStrategy` unit tests (value present at a top-level path, a
      nested path, a numeric/boolean value).
- [x] A MockMvc end-to-end test covers the replay happy path.

## Blocked by

- Slice 001 — header-strategy happy-path replay
