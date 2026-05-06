"""rename_success_data_to_annotation_metadata

Revision ID: 009570ae0cb0
Revises: 8de33cc35cd7
Create Date: 2026-04-16 17:26:16.151395

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "009570ae0cb0"
down_revision = "8de33cc35cd7"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "variant_annotation_status",
        "success_data",
        new_column_name="annotation_metadata",
        comment="Structured metadata for the annotation result",
    )


def downgrade():
    op.alter_column(
        "variant_annotation_status",
        "annotation_metadata",
        new_column_name="success_data",
        comment="Annotation results when successful",
    )
