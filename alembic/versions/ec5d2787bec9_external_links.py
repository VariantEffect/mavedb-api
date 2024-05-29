"""External links

Revision ID: ec5d2787bec9
Revises: 7a345f1bf9c3
Create Date: 2024-05-29 06:39:17.930675

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet

# revision identifiers, used by Alembic.
revision = 'ec5d2787bec9'
down_revision = '7a345f1bf9c3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("scoresets", sa.Column("external_links", postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    # Update all score sets, setting external_links to the default value.
    conn = op.get_bind()
    session = Session(bind=conn)
    session.execute(ScoreSet.__table__.update().values(external_links = {}))
    #session.commit()

    #op.alter_column("scoresets", "external_links", nullable=False)

def downgrade():
    op.drop_column("scoresets", "external_links")
