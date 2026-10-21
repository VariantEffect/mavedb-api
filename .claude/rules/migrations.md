---
paths:
  - "alembic/**"
---

# Migrations

- Generate with `poetry run alembic revision --autogenerate -m "<what changes>"`, then read the result: autogenerate misses or mangles enum changes, server defaults, views and functions.
- Set `down_revision` to the current head so the chain stays linear. Check with `poetry run alembic heads` (one head).
- Never edit a revision that has run on staging or production. Write a new one, even for a fix. The one exception is the squash at a cycle boundary (`alembic/README.md`).
- One logical change per revision. The message says what changes ("add variant_count to scoresets"), not "update model".
- Every migration must replay from an empty database inside one transaction: no imports from `mavedb.*`, no network calls, ORM sessions or commits, no `CONCURRENTLY` or `autocommit_block()`, and no `ALTER TYPE ... ADD VALUE` (enums are text with a `CHECK` constraint). `bin/check-migrations` enforces this; the rules are in `alembic/README.md`.
- Production tables are large (`variants`, `mapping_records`, `mapping_record_alleles`, `alleles`, `annotation_event` run to millions of rows). A full-table `UPDATE` holds its lock for minutes. For an index on one of them, put the `CREATE INDEX CONCURRENTLY` to run by hand before the deploy in the docstring, and create the same index with `IF NOT EXISTS` in the migration.
- Data work that needs application code or external services is an operator script in `src/mavedb/scripts/`, not a migration.
