"""Add primary publication

Revision ID: da9ba478647d
Revises: 5cad62af3705
Create Date: 2023-05-10 16:45:22.869575

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "da9ba478647d"
down_revision = "5cad62af3705"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="experiment_publication_identifiers",
        column=sa.Column("primary", sa.Boolean, nullable=True),
    )
    op.add_column(
        table_name="scoreset_publication_identifiers",
        column=sa.Column("primary", sa.Boolean, nullable=True),
    )

    # data migration to set the linked publication identifier of all existing
    # experiments/scoresets with only one linked publication identifier to be
    # the primary publication
    op.execute(
        """
        UPDATE experiment_publication_identifiers
        SET "primary" = TRUE
        WHERE
            "primary" IS NULL AND
            experiment_id IN (
                SELECT experiment_id
                FROM experiment_publication_identifiers
                GROUP BY experiment_id
                HAVING Count(*) = 1
            )
        """
    )
    op.execute(
        """
        UPDATE scoreset_publication_identifiers
        SET "primary" = TRUE
        WHERE
            "primary" IS NULL AND
            scoreset_id IN (
                SELECT scoreset_id
                FROM scoreset_publication_identifiers
                GROUP BY scoreset_id
                HAVING Count(*) = 1
            )
        """
    )


def downgrade():
    op.drop_column(table_name="experiment_publication_identifiers", column_name="primary")
    op.drop_column(table_name="scoreset_publication_identifiers", column_name="primary")
