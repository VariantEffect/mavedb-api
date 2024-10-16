"""add scoreset_fulltext materialized view

Revision ID: 0d3732aa62be
Revises: ec5d2787bec9
Create Date: 2024-10-15 14:59:16.297975

"""
from alembic import op
import sqlalchemy as sa

from alembic_utils.pg_materialized_view import PGMaterializedView

from mavedb.models.score_set import scoreset_fulltext

# revision identifiers, used by Alembic.
revision = '0d3732aa62be'
down_revision = '1d4933b4b6f7'
branch_labels = None
depends_on = None

def upgrade():
    op.create_entity(scoreset_fulltext)
    op.execute("create index scoreset_fulltext_idx on scoreset_fulltext using gin (text)")


def downgrade():
    op.execute("drop index scoreset_fulltext_idx")
    op.drop_entity(scoreset_fulltext)
