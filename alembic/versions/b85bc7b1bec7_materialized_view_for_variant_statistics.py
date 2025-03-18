"""materialized view for variant statistics
Revision ID: b85bc7b1bec7
Revises: c404b6719110
Create Date: 2025-03-14 01:53:19.898198
"""

from alembic import op
from alembic_utils.pg_materialized_view import PGMaterializedView
from sqlalchemy.dialects import postgresql

from mavedb.models.published_variant import signature, definition


# revision identifiers, used by Alembic.
revision = "b85bc7b1bec7"
down_revision = "c404b6719110"
branch_labels = None
depends_on = None


def upgrade():
    op.create_entity(
        PGMaterializedView(
            schema="public",
            signature=signature,
            definition=definition.compile(dialect=postgresql.dialect()).string,
            with_data=True,
        )
    )
    op.create_index(
        f"idx_{signature}_variant_id",
        signature,
        ["variant_id"],
        unique=False,
    )
    op.create_index(
        f"idx_{signature}_variant_urn",
        signature,
        ["variant_urn"],
        unique=False,
    )
    op.create_index(
        f"idx_{signature}_score_set_id",
        signature,
        ["score_set_id"],
        unique=False,
    )
    op.create_index(
        f"idx_{signature}_score_set_urn",
        signature,
        ["score_set_urn"],
        unique=False,
    )
    op.create_index(
        f"idx_{signature}_mapped_variant_id",
        signature,
        ["mapped_variant_id"],
        unique=True,
    )


def downgrade():
    op.drop_index(f"idx_{signature}_variant_id", signature)
    op.drop_index(f"idx_{signature}_variant_urn", signature)
    op.drop_index(f"idx_{signature}_mapped_variant_id", signature)
    op.drop_index(f"idx_{signature}_score_set_id", signature)
    op.drop_index(f"idx_{signature}_score_set_urn", signature)
    op.drop_entity(
        PGMaterializedView(
            schema="public",
            signature=signature,
            definition=definition.compile(dialect=postgresql.dialect()).string,
            with_data=True,
        )
    )
