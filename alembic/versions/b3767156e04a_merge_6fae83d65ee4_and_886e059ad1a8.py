"""merge 6fae83d65ee4 and 886e059ad1a8

Revision ID: b3767156e04a
Revises: 6fae83d65ee4, 886e059ad1a8
Create Date: 2024-04-15 11:24:54.269178

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b3767156e04a"
down_revision = ("6fae83d65ee4", "886e059ad1a8")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
