"""drop_redundant_variant_annotation_status_indexes

Revision ID: a3b7c9d1e2f4
Revises: 009570ae0cb0
Create Date: 2026-04-20 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a3b7c9d1e2f4"
down_revision = "009570ae0cb0"
branch_labels = None
depends_on = None


# The variant_annotation_status table is append-only and write-heavy.  Every
# production query filters on (variant_id, annotation_type, [version], current),
# which is fully served by the composite index
# ix_variant_annotation_status_variant_type_version_current.
#
# The 8 indexes being dropped here are either:
# - single-column prefixes of that composite (redundant),
# - on low-selectivity columns (boolean, 3 enum values), or
# - on columns that are never filtered in any query (status, created_at).
#
# Keeping: the 4-column composite + the job_run_id FK index.

INDEXES_TO_DROP = [
    "ix_variant_annotation_status_variant_id",
    "ix_variant_annotation_status_annotation_type",
    "ix_variant_annotation_status_status",
    "ix_variant_annotation_status_created_at",
    "ix_variant_annotation_variant_type_status",
    "ix_variant_annotation_type_status",
    "ix_variant_annotation_status_current",
    "ix_variant_annotation_status_version",
]

# Column definitions for downgrade (recreating dropped indexes)
INDEX_COLUMNS = {
    "ix_variant_annotation_status_variant_id": ["variant_id"],
    "ix_variant_annotation_status_annotation_type": ["annotation_type"],
    "ix_variant_annotation_status_status": ["status"],
    "ix_variant_annotation_status_created_at": ["created_at"],
    "ix_variant_annotation_variant_type_status": ["variant_id", "annotation_type", "status"],
    "ix_variant_annotation_type_status": ["annotation_type", "status"],
    "ix_variant_annotation_status_current": ["current"],
    "ix_variant_annotation_status_version": ["version"],
}


def upgrade() -> None:
    for index_name in INDEXES_TO_DROP:
        op.drop_index(index_name, table_name="variant_annotation_status")


def downgrade() -> None:
    for index_name, columns in INDEX_COLUMNS.items():
        op.create_index(index_name, "variant_annotation_status", columns)
