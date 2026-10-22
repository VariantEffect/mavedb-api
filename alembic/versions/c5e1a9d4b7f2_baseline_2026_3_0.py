"""baseline: schema as of release 2026.3.0

Revision ID: c5e1a9d4b7f2
Revises:
Create Date: 2026-10-19

Replaces every revision up to and including 2026.3.0's head, keeping that head's ID so databases already
there need no restamp. Builds the schema and seed vocabulary from ``alembic/baseline/c5e1a9d4b7f2.sql``,
written by ``bin/build-baseline`` from production. See ``alembic/README.md``.
"""

from pathlib import Path

from alembic import op

# revision identifiers, used by Alembic.
revision = "c5e1a9d4b7f2"
down_revision = None
branch_labels = None
depends_on = None

BASELINE = Path(__file__).resolve().parent.parent / "baseline" / f"{revision}.sql"


def upgrade():
    # Function bodies in the dump can reference tables created later in it. The upgrade runs in one
    # transaction, so the setting is restored for the migrations that follow.
    op.execute("SET LOCAL check_function_bodies = false")
    # Executed by the driver directly, so SQLAlchemy doesn't read ``:name`` in the dump as a bind parameter.
    op.get_bind().exec_driver_sql(BASELINE.read_text())
    op.execute("SET LOCAL check_function_bodies = DEFAULT")


def downgrade():
    raise NotImplementedError("The baseline can't be downgraded. Restore from a snapshot instead.")
