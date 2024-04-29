"""Add is_first_login Column to User Model.

Revision ID: 8e26f1a1160d
Revises: 377bb8c30bde
Create Date: 2024-04-29 11:15:07.067857

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8e26f1a1160d"
down_revision = "377bb8c30bde"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("users", sa.Column("is_first_login", sa.Boolean(), nullable=False, server_default="False"))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("users", "is_first_login")
    # ### end Alembic commands ###