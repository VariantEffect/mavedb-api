"""index functional classification variant fk

variant_id is only the trailing column of score_calibration_functional_classification_variants'
composite primary key, so Postgres has no index it can use to look up rows by variant_id alone.
Every Variant delete (including the per-variant cascade when a score set is deleted) forces a
sequential scan of this table to enforce that FK, which dominates the delete for any score set
with a nontrivial number of variants. See mavedb-api#677 ("unpublished score set deletion often
fails").

Building it takes a lock that blocks writes to the table, so in a populated database build it by hand
first, which makes this migration a no-op:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_score_calib_func_classification_variants_variant_id
        ON score_calibration_functional_classification_variants (variant_id);

Revision ID: 15c40c367731
Revises: c4b18d0f7a92
Create Date: 2026-09-21 11:19:39.396343

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "15c40c367731"
down_revision = "c4b18d0f7a92"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_score_calib_func_classification_variants_variant_id"
TABLE_NAME = "score_calibration_functional_classification_variants"


def upgrade():
    op.execute(f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON {TABLE_NAME} (variant_id)")


def downgrade():
    op.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
