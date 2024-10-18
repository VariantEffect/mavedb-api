"""External links

Revision ID: ec5d2787bec9
Revises: 7a345f1bf9c3
Create Date: 2024-05-29 06:39:17.930675

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "ec5d2787bec9"
down_revision = "7a345f1bf9c3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "scoresets",
        sa.Column("external_links", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("scoresets", "external_links")
