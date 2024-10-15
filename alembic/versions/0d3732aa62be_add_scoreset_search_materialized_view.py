"""add scoreset_search materialized view

Revision ID: 0d3732aa62be
Revises: ec5d2787bec9
Create Date: 2024-10-15 14:59:16.297975

"""
from alembic import op
import sqlalchemy as sa

from alembic_utils.pg_materialized_view import PGMaterializedView

scoreset_search_query = """
    select S.id, to_tsvector(S.title || ' ' || S.short_description || ' ' || S.abstract_text || ' ' || string_agg(G.name, ' ')) as text
    from scoresets S join target_genes G on G.scoreset_id = S.id
    group by S.id
"""

# revision identifiers, used by Alembic.
revision = '0d3732aa62be'
down_revision = '1d4933b4b6f7'
branch_labels = None
depends_on = None

scoreset_search = PGMaterializedView(
    schema="public",
    signature="scoreset_search",
    definition=scoreset_search_query,
    with_data=True
)


def upgrade():
    op.create_entity(scoreset_search)
    op.execute("create index scoreset_search_idx on scoreset_search using gin (text)")


def downgrade():
    op.execute("drop index scoreset_search_idx")
    op.drop_entity(scoreset_search)
