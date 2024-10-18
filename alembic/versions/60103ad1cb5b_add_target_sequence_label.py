"""Add Target Sequence Label

Revision ID: 60103ad1cb5b
Revises: 194cfebabe32
Create Date: 2023-08-29 16:04:44.620385

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "60103ad1cb5b"
down_revision = "194cfebabe32"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("target_sequences", sa.Column("label", sa.String(), nullable=True))


def downgrade():
    op.drop_column("target_sequences", "label")
