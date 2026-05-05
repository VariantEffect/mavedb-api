"""add position column to collection associations for ordering

Revision ID: e1f2a3b4c5d6
Revises: dcf8572d3a17
Create Date: 2026-02-13 10:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e1f2a3b4c5d6"
down_revision = "dcf8572d3a17"
branch_labels = None
depends_on = None


def upgrade():
    """
    Add position column to collection association tables to support user-controlled ordering.
    Backfills existing rows with sequential positions ordered by the foreign key ID.
    """
    # 1. Add column with a temporary server default so existing rows are valid.
    op.add_column("collection_score_sets", sa.Column("position", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("collection_experiments", sa.Column("position", sa.Integer(), nullable=False, server_default="0"))

    # 2. Backfill: assign sequential positions within each collection, ordered by score_set_id / experiment_id ASC.
    #    This gives existing rows a deterministic order rather than all sharing position 0.
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
        UPDATE collection_score_sets AS css
        SET position = sub.rn
        FROM (
            SELECT collection_id, score_set_id,
                   ROW_NUMBER() OVER (PARTITION BY collection_id ORDER BY score_set_id) - 1 AS rn
            FROM collection_score_sets
        ) AS sub
        WHERE css.collection_id = sub.collection_id AND css.score_set_id = sub.score_set_id
    """
        )
    )
    conn.execute(
        sa.text(
            """
        UPDATE collection_experiments AS ce
        SET position = sub.rn
        FROM (
            SELECT collection_id, experiment_id,
                   ROW_NUMBER() OVER (PARTITION BY collection_id ORDER BY experiment_id) - 1 AS rn
            FROM collection_experiments
        ) AS sub
        WHERE ce.collection_id = sub.collection_id AND ce.experiment_id = sub.experiment_id
    """
        )
    )

    # 3. Remove server defaults — the ORM model's `default=0` handles new inserts.
    op.alter_column("collection_score_sets", "position", server_default=None)
    op.alter_column("collection_experiments", "position", server_default=None)


def downgrade():
    """Remove position columns from collection association tables."""
    op.drop_column("collection_experiments", "position")
    op.drop_column("collection_score_sets", "position")
