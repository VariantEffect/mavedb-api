"""Index score set variants by variant number

Score set CSV queries order rows by the number after '#' in the variant URN (``Variant.variant_number``). Without an
index on that expression, every request sorts the whole score set, and a page deep into a large score set takes
minutes.

The index evaluates the expression for every row, so every non-null variant URN must end in '#<integer>'; an insert
that breaks that fails. No existing URN broke it when this migration was written.

Revision ID: f6752571d14d
Revises: c4b18d0f7a92
Create Date: 2026-09-29 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f6752571d14d"
down_revision = "c4b18d0f7a92"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_variants_scoreset_id_variant_number"


def upgrade():
    # Built concurrently so variant writes continue during the build, which must run outside a transaction. A failed
    # concurrent build leaves an invalid index behind, so drop any leftover before building.
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
        op.execute(
            f"CREATE INDEX CONCURRENTLY {INDEX_NAME} "
            "ON variants (scoreset_id, (CAST(split_part(urn, '#', 2) AS INTEGER)), id)"
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
