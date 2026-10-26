"""Add calibration version links.

Revision ID: c1bdc0bd24db
Revises: e59e1cc49c9c
Create Date: 2026-10-26
"""

import sqlalchemy as sa

from alembic import op

revision = "c1bdc0bd24db"
down_revision = "e59e1cc49c9c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("score_calibrations", sa.Column("replaces_id", sa.Integer(), nullable=True))
    op.create_index("ix_score_calibrations_replaces_id", "score_calibrations", ["replaces_id"], unique=True)
    op.create_foreign_key(
        "fk_score_calibrations_replaces_id", "score_calibrations", "score_calibrations", ["replaces_id"], ["id"]
    )


def downgrade():
    op.drop_constraint("fk_score_calibrations_replaces_id", "score_calibrations", type_="foreignkey")
    op.drop_index("ix_score_calibrations_replaces_id", table_name="score_calibrations")
    op.drop_column("score_calibrations", "replaces_id")
