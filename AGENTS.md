# AGENTS.md — MaveDB API

Facts about this repo that you can't work out from the code. Keep this file short. Guidance for specific paths goes in `.claude/rules/`, which loads when you touch those paths. Rules shared with the other MaveDB repos are in the workspace `AGENTS.md` one folder up. Update all of them in the same change that makes any of it untrue.

The MaveDB API is a FastAPI app plus an ARQ/Redis worker over PostgreSQL. The web app and the user docs live in `VariantEffect/mavedb-ui`.

## Setup

- `poetry install --with dev --extras server`, on Python 3.11 or 3.12.
- Run everything through `poetry run`. The host Python has none of the dependencies.
- Tests need PostgreSQL server binaries on `PATH`, because pytest-postgresql runs `pg_ctl`, which refuses to run as root. With `bin/test-db up` they need Docker instead. Server-extras tests also need `tabix`.

## Commands

| Task | Command |
|---|---|
| Lint, format check, types, migration rules | `bin/check` |
| Start the test database (Postgres on tmpfs in Docker) | `bin/test-db up` (`bin/test-db down` stops it) |
| Targeted tests | `bin/t tests/routers/test_score_set.py -k publish` |
| What PR CI runs | `bin/t tests/ -n logical -m "not network and not slow"` |
| Format | `poetry run ruff format` |
| Migrations | `poetry run alembic upgrade head` |
| Operator scripts | `poetry run python -m mavedb.scripts.<name>` (dry run unless `--commit`) |

- Run `bin/check` after every edit. It takes seconds.
- Run `bin/test-db up` before testing when Docker is available. `bin/t` uses it automatically, which keeps the thousands of files each test database creates off the host disk; without it, test runs load macOS's file watchers and endpoint scanning heavily. One container serves every checkout and worktree.
- Before handing work back, run the targeted tests for what you touched. Add `-n 6` when running more than a couple of files; xdist costs a few seconds to start, so skip it for one file. The PR set is about 5,400 tests and takes about 2.5 minutes with `bin/test-db up` and `-n 6`.
- If the pre-commit hook rewrites files, re-stage them and commit again. Never use `--no-verify`.
- Change Ruff's version or rule set only in a commit of its own.

## Rules

- Private records return 404, never 403, so a response doesn't reveal that they exist.
- A route's permission check covers only the entity it names. Anything a read fans out to needs its own check through a `Viewer` (see `.claude/rules/permissions.md`).
- Write `def` by default. Use `async def` only where the code can't work without an await, keep database work out of it, and say why in a comment; `tests/routers/test_async_routes.py` enforces the database part (#881, #950).
- Changes reachable from a route or a serving `lib/` module need tests, and so do scripts that emit a recurring public artifact such as the data dump. One-shot operator scripts don't.
- Never add `slow` or `network` markers to get green; those markers drop a test from PR CI.
- Run operator scripts with `--commit` only against a local database unless asked. Never commit to production or staging databases without explicit permission.
- Never write to `mapped_variants` or `variant_translations`; they're legacy and read-only, and new mapping data goes to mapping records and alleles.
