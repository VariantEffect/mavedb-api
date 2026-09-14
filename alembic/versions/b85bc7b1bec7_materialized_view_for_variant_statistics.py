"""materialized view for variant statistics
Revision ID: b85bc7b1bec7
Revises: c404b6719110
Create Date: 2025-03-14 01:53:19.898198

The MV SQL is inlined as a frozen literal rather than imported from the model. At this revision the
MV joins the (now legacy) ``mapped_variants`` table; a later migration rebuilds it onto the
``mapping_records`` substrate. Importing the live model definition here would make a fresh replay
build *that* later shape at this early revision — before the substrate tables exist — and fail.
Freezing the historical SQL keeps replay green; the rebuild happens in its own migration.
"""

from alembic import op
from alembic_utils.pg_materialized_view import PGMaterializedView


# revision identifiers, used by Alembic.
revision = "b85bc7b1bec7"
down_revision = "c404b6719110"
branch_labels = None
depends_on = None

SIGNATURE = "published_variants_materialized_view"

# Historical definition, frozen at this revision. Do not edit to track the model.
DEFINITION = """
SELECT variants.id AS variant_id, variants.urn AS variant_urn, mapped_variants.id AS mapped_variant_id,
    scoresets.id AS score_set_id, scoresets.urn AS score_set_urn, scoresets.published_date AS published_date,
    mapped_variants.current AS current_mapped_variant
FROM variants LEFT OUTER JOIN mapped_variants ON variants.id = mapped_variants.variant_id
    JOIN scoresets ON scoresets.id = variants.scoreset_id
WHERE scoresets.published_date IS NOT NULL
"""


def upgrade():
    op.create_entity(
        PGMaterializedView(
            schema="public",
            signature=SIGNATURE,
            definition=DEFINITION,
            with_data=True,
        )
    )
    op.create_index(
        f"idx_{SIGNATURE}_variant_id",
        SIGNATURE,
        ["variant_id"],
        unique=False,
    )
    op.create_index(
        f"idx_{SIGNATURE}_variant_urn",
        SIGNATURE,
        ["variant_urn"],
        unique=False,
    )
    op.create_index(
        f"idx_{SIGNATURE}_score_set_id",
        SIGNATURE,
        ["score_set_id"],
        unique=False,
    )
    op.create_index(
        f"idx_{SIGNATURE}_score_set_urn",
        SIGNATURE,
        ["score_set_urn"],
        unique=False,
    )
    op.create_index(
        f"idx_{SIGNATURE}_mapped_variant_id",
        SIGNATURE,
        ["mapped_variant_id"],
        unique=True,
    )


def downgrade():
    op.drop_index(f"idx_{SIGNATURE}_variant_id", SIGNATURE)
    op.drop_index(f"idx_{SIGNATURE}_variant_urn", SIGNATURE)
    op.drop_index(f"idx_{SIGNATURE}_mapped_variant_id", SIGNATURE)
    op.drop_index(f"idx_{SIGNATURE}_score_set_id", SIGNATURE)
    op.drop_index(f"idx_{SIGNATURE}_score_set_urn", SIGNATURE)
    op.drop_entity(
        PGMaterializedView(
            schema="public",
            signature=SIGNATURE,
            definition=DEFINITION,
            with_data=True,
        )
    )
