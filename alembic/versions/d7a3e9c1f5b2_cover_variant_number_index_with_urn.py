"""cover the variant number index with urn

Score set CSV pages select their variant ids in variant-number order before fetching full rows. Postgres won't use
an index-only scan on an expression index unless the expression's columns are in the index too, so without ``urn``
every row before ``start`` is still read from the table. This replaces ``ix_variants_scoreset_number`` with the same
index plus ``INCLUDE (urn)``.

Building it takes a lock that blocks writes to ``variants``, so in a populated database build it and drop the old one
by hand first, which makes this migration a no-op:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_variants_scoreset_number_covering
        ON variants (scoreset_id, (CAST(split_part(urn, '#', 2) AS INTEGER)), id) INCLUDE (urn);
    DROP INDEX CONCURRENTLY IF EXISTS ix_variants_scoreset_number;

Revision ID: d7a3e9c1f5b2
Revises: c5e1a9d4b7f2
Create Date: 2026-10-20

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d7a3e9c1f5b2"
down_revision = "c5e1a9d4b7f2"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_variants_scoreset_number_covering "
        "ON variants (scoreset_id, (CAST(split_part(urn, '#', 2) AS INTEGER)), id) INCLUDE (urn)"
    )
    op.execute("DROP INDEX IF EXISTS ix_variants_scoreset_number")


def downgrade():
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_variants_scoreset_number "
        "ON variants (scoreset_id, (CAST(split_part(urn, '#', 2) AS INTEGER)), id)"
    )
    op.execute("DROP INDEX IF EXISTS ix_variants_scoreset_number_covering")
