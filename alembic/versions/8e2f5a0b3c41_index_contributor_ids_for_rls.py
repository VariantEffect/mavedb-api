"""index contributor_id on the three contributor tables and score_set_id on score_calibrations

The RLS policies (#809) look up the records a caller contributes to by ``contributor_id``, but each contributor
table's primary key leads with the parent's id, so the lookup can't use it. The calibration policy joins on
``score_calibrations.score_set_id``. All four tables are small, so the indexes build without ``CONCURRENTLY``.

Revision ID: 8e2f5a0b3c41
Revises: 7c1d4e9a2b30
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "8e2f5a0b3c41"
down_revision = "7c1d4e9a2b30"
branch_labels = None
depends_on = None

INDEXES = [
    ("ix_scoreset_contributors_contributor_id", "scoreset_contributors", "contributor_id"),
    ("ix_experiment_contributors_contributor_id", "experiment_contributors", "contributor_id"),
    ("ix_experiment_set_contributors_contributor_id", "experiment_set_contributors", "contributor_id"),
    ("ix_score_calibrations_score_set_id", "score_calibrations", "score_set_id"),
]


def upgrade():
    for name, table, column in INDEXES:
        op.create_index(name, table, [column])


def downgrade():
    for name, table, _ in reversed(INDEXES):
        op.drop_index(name, table_name=table)
