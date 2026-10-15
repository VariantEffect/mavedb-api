"""store a binned score distribution on each score set

Revision ID: b4d8f2a6c1e3
Revises: a7c3e5f9b1d2
Create Date: 2026-10-08

Adds a nullable score_distribution JSONB column, written by the variant creation job (see
mavedb.lib.score_distribution). Adding a nullable column without a default is a catalog-only change and
does not rewrite the table. Existing score sets stay null until the backfill runs:

    python -m mavedb.scripts.backfill_score_distributions --commit
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b4d8f2a6c1e3"
down_revision = "a7c3e5f9b1d2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("scoresets", sa.Column("score_distribution", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade():
    op.drop_column("scoresets", "score_distribution")
