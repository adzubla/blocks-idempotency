# Issue tracker: Local Markdown

Issues live as markdown files in `docs/issues/`.

## Conventions

- `docs/issues/INDEX.md` is the map: a table of `# | Slice | Blocked by`, one row per issue, linking to the issue file.
- Each issue is `docs/issues/<NNN>-<slug>.md`, numbered from `001` (e.g. `001-header-strategy-happy-path.md`).
- Each issue file opens with `> Source: <PRD path> · Type: <AFK|...>`, followed by `## What to build`, `## Acceptance criteria` (checklist), and `## Blocked by`.
- Triage state is recorded as a `Status:` line near the top of the issue file — see `triage-labels.md` for the role strings.
- Comments and conversation history append to the bottom of the file under a `## Comments` heading.

## When a skill says "publish to the issue tracker"

Create a new file at `docs/issues/<NNN>-<slug>.md` (next available number), and add a row to `docs/issues/INDEX.md`.

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path or number under `docs/issues/`.
