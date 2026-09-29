"""Index score set variants by variant number

Score set CSV queries order rows by the number after '#' in the variant URN (``Variant.variant_number``). Without an
index on that expression, every request sorts the whole score set, and a page deep into a large score set takes
minutes.

The index evaluates the expression for every row. A URN with no '#' gets a null number; one whose text after '#' isn't
an integer fails the insert. No existing URN did when this migration was written.

Building it takes a lock that blocks writes to ``variants``, so in a populated database build it by hand first, which
makes this migration a no-op:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_variants_scoreset_number
        ON variants (scoreset_id, (CAST(NULLIF(split_part(urn, '#', 2), '') AS INTEGER)), id);

Revision ID: f6752571d14d
Revises: 15c40c367731
Create Date: 2026-09-29 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f6752571d14d"
down_revision = "15c40c367731"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_variants_scoreset_number "
        "ON variants (scoreset_id, (CAST(NULLIF(split_part(urn, '#', 2), '') AS INTEGER)), id)"
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_variants_scoreset_number")
