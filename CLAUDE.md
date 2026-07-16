## Agent skills

### Issue tracker

Issues are tracked as local markdown files under `docs/issues/` (numbered slice files + an `INDEX.md` map table); no GitHub/GitLab remote is configured, so PRs aren't a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default names, recorded as a `Status:` line in each issue file. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Running tests in isolation

`blocks-idempotency-store-postgres` and `-redis` depend on `blocks-idempotency-core`'s test-jar (the shared `IdempotencyStoreContractTest` suite). Running `-Dtest=` scoped to one of those modules **without `-am`** (or from inside the submodule directory) resolves that test-jar from `~/.m2` instead of the working tree — silently stale if `core`'s tests changed since the last install, which reads as "fails/passes differently in isolation than in the full module suite." Always run `mvn test -pl <module> -am -Dtest=...`, or `mvn install` once after touching anything under `blocks-idempotency-core`, before trusting an isolated test run.
