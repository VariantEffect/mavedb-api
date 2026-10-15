"""index annotation_event.score_set_id

Revision ID: c9b2a4f8e1d3
Revises: b8f2c5a1d3e4
Create Date: 2026-06-30

Adds the missing foreign-key index on annotation_event.score_set_id. Every other FK on this table
is indexed; score_set_id was not. The index backs the ON DELETE SET NULL cascade fired when a score
set is deleted (an unindexed FK forces a sequential scan of the event log per deleted score set) and
any operator/BI query that filters the log by score set.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c9b2a4f8e1d3"
down_revision = "b8f2c5a1d3e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_annotation_event_score_set_id", "annotation_event", ["score_set_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_annotation_event_score_set_id", table_name="annotation_event")
