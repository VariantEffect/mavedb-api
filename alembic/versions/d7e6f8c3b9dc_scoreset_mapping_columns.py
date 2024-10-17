"""scoreset_mapping_columns

Revision ID: d7e6f8c3b9dc
Revises: f36cf612e029
Create Date: 2024-08-28 09:54:08.249077

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "d7e6f8c3b9dc"
down_revision = "f36cf612e029"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "scoresets",
        sa.Column(
            "mapping_state",
            sa.Enum(
                "incomplete",
                "processing",
                "failed",
                "complete",
                "pending_variant_processing",
                "not_attempted",
                "queued",
                name="mappingstate",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=True,
        ),
    )
    op.add_column("scoresets", sa.Column("mapping_errors", postgresql.JSONB, nullable=True))


def downgrade():
    op.drop_constraint("mappingstate", table_name="scoresets")
    op.drop_column("scoresets", "mapping_state")
    op.drop_column("scoresets", "mapping_errors")
