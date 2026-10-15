---
paths:
  - "alembic/**"
---

# Migrations

- Generate with `poetry run alembic revision --autogenerate -m "<what changes>"`, then read the result: autogenerate misses or mangles enum changes, server defaults, views and functions.
- Set `down_revision` to the current head so the chain stays linear. Check with `poetry run alembic heads` (one head).
- Never edit a revision that has run on staging or production. Write a new one, even for a fix.
- One logical change per revision. The message says what changes ("add variant_count to scoresets"), not "update model".
- Production tables are large (`variants`, `mapping_records`, `mapping_record_alleles`, `alleles`, `annotation_event` run to millions of rows). A full-table `UPDATE` holds its lock for minutes. Build indexes on these tables `CONCURRENTLY` inside `op.get_context().autocommit_block()`, as `a7c3e5f9b1d2_unique_allele_hgvs_expression.py` does.
- Data migrations with no clean upgrade/downgrade path go in `alembic/manual_migrations/`. An operator runs them by hand (see the README there); Ruff excludes the directory.
