"""Add urn_redirects, forwarding URNs that publication has retired

Revision ID: c4b18d0f7a92
Revises: a7f3c2e9b104
Create Date: 2026-09-01 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c4b18d0f7a92"
down_revision = "a7f3c2e9b104"
branch_labels = None
depends_on = None


def upgrade():
    # Not backfilled: publication overwrote each record's temporary URN in place and kept no history of
    # it, so the URNs retired before this table existed cannot be recovered.
    op.create_table(
        "urn_redirects",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("old_urn", sa.String(length=64), nullable=False),
        sa.Column("new_urn", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_urn_redirects_old_urn", "urn_redirects", ["old_urn"], unique=True)


def downgrade():
    op.drop_index("ix_urn_redirects_old_urn", table_name="urn_redirects")
    op.drop_table("urn_redirects")
