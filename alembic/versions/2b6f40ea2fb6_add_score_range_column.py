"""Add score range column

Revision ID: 2b6f40ea2fb6
Revises: 1d4933b4b6f7
Create Date: 2024-09-09 12:25:33.180077

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "2b6f40ea2fb6"
down_revision = "1d4933b4b6f7"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("scoresets", sa.Column("score_ranges", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("scoresets", "score_ranges")
    # ### end Alembic commands ###
