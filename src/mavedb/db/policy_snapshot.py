"""Write the database's row-level security rules as one readable SQL file (#809).

Policies live in migrations, so the current rule for a table is spread across the history. The snapshot,
``src/mavedb/db/policies.sql``, is the readable copy, generated from a database built by ``alembic upgrade
head`` and never edited by hand. A test replays the migrations and fails when the committed file differs, so a
change to a policy is reviewed as a diff of this file.

The test schema is built from the models, which can't declare policies, so the test template loads this file
to get them.
"""

from pathlib import Path

from sqlalchemy import Connection, text

SNAPSHOT_PATH = Path(__file__).parent / "policies.sql"

# Every function a policy calls. Policy bodies call nothing else.
RLS_FUNCTIONS = ("mavedb_user_id", "mavedb_roles")

_HEADER = """\
-- Row-level security in the public schema: the functions it relies on, then each table's RLS state and
-- policies.
-- Generated from a database at alembic head by `python -m mavedb.scripts.write_policy_snapshot`. Don't edit
-- by hand: change policies in a migration and regenerate this file.
"""


def render_policy_snapshot(conn: Connection) -> str:
    """Return the snapshot of ``conn``'s database, sorted so an unchanged schema renders identically."""
    functions = conn.execute(
        text(
            "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'public' AND p.proname = ANY(:names) ORDER BY p.proname, p.oid::regprocedure::text"
        ),
        {"names": list(RLS_FUNCTIONS)},
    ).scalars()
    triggers = conn.execute(
        text(
            "SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid "
            "WHERE NOT t.tgisinternal AND p.proname = ANY(:names) ORDER BY t.tgrelid::regclass::text, t.tgname"
        ),
        {"names": list(RLS_FUNCTIONS)},
    ).scalars()
    tables = conn.execute(
        text(
            "SELECT c.relname, c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND (c.relrowsecurity OR EXISTS (SELECT FROM pg_policy p WHERE p.polrelid = c.oid)) "
            "ORDER BY c.relname"
        )
    ).all()
    policies = conn.execute(
        text(
            "SELECT tablename, policyname, permissive, roles::text[], cmd, qual, with_check FROM pg_policies "
            "WHERE schemaname = 'public' ORDER BY tablename, policyname"
        )
    ).all()

    sections = [_HEADER]
    sections.extend(f"{definition.strip()};\n" for definition in functions)
    if trigger_lines := "".join(f"{trigger};\n" for trigger in triggers):
        sections.append(trigger_lines)
    for table, enabled in tables:
        state = "ENABLED" if enabled else "not enabled"
        lines = [f"-- {table}: row-level security {state}"]
        if enabled:
            lines.append(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        for _, name, permissive, roles, command, using, check in (p for p in policies if p.tablename == table):
            statement = f"CREATE POLICY {name} ON {table} AS {permissive} FOR {command} TO {', '.join(sorted(roles))}"
            if using is not None:
                statement += f"\n  USING ({using})"
            if check is not None:
                statement += f"\n  WITH CHECK ({check})"
            lines.append(statement + ";")
        sections.append("\n".join(lines) + "\n")
    return "\n".join(sections)
