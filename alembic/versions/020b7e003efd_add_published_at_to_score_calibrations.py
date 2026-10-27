"""Add published_at to score_calibrations.

Existing published calibrations keep a null published_at: their publication time was never recorded, and
modification_date also moves when controls are removed, so it can't stand in.

Revision ID: 020b7e003efd
Revises: c1bdc0bd24db
Create Date: 2026-10-27
"""

from alembic import op
import sqlalchemy as sa

revision = "020b7e003efd"
down_revision = "c1bdc0bd24db"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("score_calibrations", sa.Column("published_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_column("score_calibrations", "published_at")
