"""rename calibration publication relation classification to evidence

Revision ID: 659999dec5d9
Revises: e1f2a3b4c5d6
Create Date: 2026-02-23 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "659999dec5d9"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        UPDATE score_calibration_publication_identifiers
        SET relation = 'evidence'
        WHERE relation = 'classification'
        """
    )


def downgrade():
    op.execute(
        """
        UPDATE score_calibration_publication_identifiers
        SET relation = 'classification'
        WHERE relation = 'evidence'
        """
    )
