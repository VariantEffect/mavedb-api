"""Add calibration version links.

Revision ID: c1bdc0bd24db
Revises: 6987f253e02a
Create Date: 2026-10-26
"""

from alembic import op
import sqlalchemy as sa

revision = "c1bdc0bd24db"
down_revision = "6987f253e02a"
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
