# Migrations

`alembic upgrade head` must build a working database from an empty one, and every upgrade must apply all or nothing. CI checks both: it replays the history on an empty Postgres for every PR, and `bin/check-migrations` (run by `bin/check`) fails on the patterns below.

## Rules for new migrations

- No imports from `mavedb.*`. Write views and functions as literal SQL, so replaying a migration builds what it shipped with, not what the models say today.
- No network calls, ORM sessions or commits. Data that needs either belongs in a script in `src/mavedb/scripts/`, run by an operator.
- Nothing that can't run inside a transaction, such as `CREATE INDEX CONCURRENTLY` or `autocommit_block()`.
- No `ALTER TYPE ... ADD VALUE`. The replay runs every migration in one transaction, and Postgres won't use an enum value in the transaction that added it. Store enums as text with a `CHECK` constraint, as the existing ones are.

### Large indexes

Building an index on a large table without `CONCURRENTLY` holds a lock for minutes. Build it by hand in production before the deploy:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_name ON big_table (column);
```

The migration then creates the same index without `CONCURRENTLY` and with `IF NOT EXISTS`. In production that does nothing; on an empty database it builds the index. Put the by-hand statement in the migration's docstring, which the check ignores.

## The baseline

The oldest revision is a baseline: one migration that builds the schema as of a release, from `alembic/baseline/<revision>.sql`. It keeps that release's head revision ID, so databases already at that release need no restamp. It can't be downgraded; the rollback across it is a database snapshot.

The SQL file comes from a schema-only dump of production plus the fixed vocabulary an empty database needs (`SEED_TABLES` and `SEED_ROWS` in `bin/build-baseline`). Don't edit it by hand.

## Squashing

At each cycle boundary, once the last release has been stable in production for a few days, squash up to its head:

1. Confirm production and staging both report the release's head in `alembic_version`, and that no open branch's migration is older than it.
2. Write the baseline from production, as the master user through the bastion tunnel:

    ```bash
    bin/build-baseline <head revision> "host=localhost port=15432 dbname=mavedb user=postgres sslmode=require"
    ```

3. Replace the head revision's file with a baseline migration (copy the current one), and delete every older revision and the previous baseline's SQL file. Migrations after the head already point at its ID.
4. Check the replay: run `alembic upgrade head` on an empty database, and diff a schema-only dump of it against production's.
5. Re-parent the first migration on each open branch onto the baseline, and add a row to the log below.

Ownership and grants aren't in the baseline. The role bootstrap's grants step applies them (#827).

## Roles

Migrations run as `mavedb_migrate`, which runs as `mavedb_owner`, so every object a migration creates belongs to the owner. Every upgrade, downgrade or stamp then runs the bootstrap's grants step in the same transaction (`alembic/env.py`). It revokes every grant on the owner's tables, views, sequences and functions, and grants exactly what `src/mavedb/db/rls.py` lists, so the grants never depend on the migration history, and a squash can't lose one.

So a migration never grants or revokes (`bin/check-migrations` refuses it). A new table, view or function gets no privileges until it has an entry in `rls.py`, which also says whether row-level security decides which rows the api sees. CI fails while anything in the schema has no entry.

The upgrade stops if the database was never set up for the roles, or if a role has privileges the bootstrap doesn't give it. A new database, including a local one, needs setting up once, as the master user (`postgres` locally):

```bash
poetry run python -m mavedb.scripts.bootstrap_roles setup --commit
```

A migration may create only what `mavedb_owner` can own: tables, views, functions, policies and indexes. Extensions, roles and cluster or database settings need the master user, so they go in the bootstrap's cluster or database part, run before the migration that relies on them. `bin/build-baseline` leaves extensions out of the baseline for the same reason.

### Restoring a dump

Loading a data-only dump with `session_replication_role = replica`, which skips foreign key checks and triggers, is a master user's job. None of the four roles may set it. Locally `postgres` is a superuser. On RDS the master user sets it, then borrows the owner to write its tables and returns it afterwards:

```sql
SET session_replication_role = replica;
GRANT mavedb_owner TO CURRENT_USER;
SET ROLE mavedb_owner;
-- load the dump
RESET ROLE;
REVOKE mavedb_owner FROM CURRENT_USER;
```

## Squash log

| Baseline revision | Release | Squashed on |
|---|---|---|
| `c5e1a9d4b7f2` | 2026.3.0 | 2026-10-19 |
