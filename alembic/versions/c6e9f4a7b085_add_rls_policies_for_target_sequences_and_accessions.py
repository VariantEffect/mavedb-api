"""add RLS policies to target_sequences and target_accessions, by the score set they carry

A sequence or accession is visible exactly when its score set is. The score set row is written before its
targets, so a new one passes the policy when it's inserted, as ``INSERT ... RETURNING`` requires. Each table also
gets a worker policy. Nothing is enforced until row-level security is enabled (#825, #878).

Revision ID: c6e9f4a7b085
Revises: b5d8e3f6a974
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c6e9f4a7b085"
down_revision = "b5d8e3f6a974"
branch_labels = None
depends_on = None

TABLES = ("target_sequences", "target_accessions")


def upgrade():
    for table in TABLES:
        predicate = f"EXISTS (SELECT 1 FROM scoresets p WHERE p.id = {table}.score_set_id)"
        op.execute(f"CREATE POLICY {table}_rule ON {table} FOR ALL USING ({predicate}) WITH CHECK ({predicate})")
        op.execute(f"CREATE POLICY {table}_worker ON {table} FOR ALL TO mavedb_worker USING (true) WITH CHECK (true)")


def downgrade():
    for table in reversed(TABLES):
        op.execute(f"DROP POLICY {table}_worker ON {table}")
        op.execute(f"DROP POLICY {table}_rule ON {table}")
