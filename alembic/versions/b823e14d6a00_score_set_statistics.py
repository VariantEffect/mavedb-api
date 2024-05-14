"""Score set statistics

Revision ID: b823e14d6a00
Revises: 9702d32bacb3
Create Date: 2024-05-12 18:37:23.203099

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b823e14d6a00'
down_revision = '9702d32bacb3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('scoresets', sa.Column('statistics', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade():
    op.drop_column('scoresets', 'statistics')
