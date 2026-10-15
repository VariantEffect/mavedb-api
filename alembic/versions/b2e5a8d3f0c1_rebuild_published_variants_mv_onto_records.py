"""rebuild published_variants MV onto the mapping_records substrate

Revision ID: b2e5a8d3f0c1
Revises: a1d4f7c2e9b0
Create Date: 2026-07-18

Rebuilds the ``published_variants_materialized_view`` off the legacy ``mapped_variants`` join onto the
``mapping_records`` substrate (part of the MappedVariant read-path teardown). The MV keeps its grain
(one row per published variant per mapping-record version, unmapped variants retained with a NULL
mapping id) and its published filter. Column changes:
  * ``mapped_variant_id`` -> ``mapping_record_id``
  * ``current_mapped_variant`` -> ``current_mapping_record`` (``valid_to IS NULL``)

Both directions are frozen inline as literal SQL — the migration imports nothing from the model layer.
A migration is an immutable historical record: importing ``published_variant.definition`` would couple
this fixed revision to mutable code, so a later model edit would make a replay of *this* revision build
that later shape (possibly against tables that no longer exist by then) and fail. ``_UPGRADE_DEFINITION``
below was compiled once from the model at authoring time and must not be edited to track the model.
"""

from alembic_utils.pg_materialized_view import PGMaterializedView

from alembic import op

# revision identifiers, used by Alembic.
revision = "b2e5a8d3f0c1"
down_revision = "a1d4f7c2e9b0"
branch_labels = None
depends_on = None

SIGNATURE = "published_variants_materialized_view"

# New (mapping_records-based) definition, frozen at this revision. Compiled from the model once at
# authoring time; do not edit to track the model. The LEFT OUTER JOIN to mapping_records carries no
# liveness filter on purpose — every record version is retained and current-ness is exposed via the
# ``current_mapping_record`` flag (``valid_to IS NULL``).
_UPGRADE_DEFINITION = """
SELECT variants.id AS variant_id, variants.urn AS variant_urn, mapping_records.id AS mapping_record_id,
    scoresets.id AS score_set_id, scoresets.urn AS score_set_urn, scoresets.published_date AS published_date,
    mapping_records.valid_to IS NULL AS current_mapping_record
FROM variants LEFT OUTER JOIN mapping_records ON variants.id = mapping_records.variant_id
    JOIN scoresets ON scoresets.id = variants.scoreset_id
WHERE scoresets.published_date IS NOT NULL
"""

# Legacy (mapped_variants-based) definition, inlined for the downgrade path. ``mapped_variants`` still
# exists at this revision (dropped in a later teardown migration), so downgrade remains runnable.
_LEGACY_DEFINITION = """
SELECT variants.id AS variant_id, variants.urn AS variant_urn, mapped_variants.id AS mapped_variant_id,
    scoresets.id AS score_set_id, scoresets.urn AS score_set_urn, scoresets.published_date AS published_date,
    mapped_variants.current AS current_mapped_variant
FROM variants LEFT OUTER JOIN mapped_variants ON variants.id = mapped_variants.variant_id
    JOIN scoresets ON scoresets.id = variants.scoreset_id
WHERE scoresets.published_date IS NOT NULL
"""


def upgrade():
    # Dropping the MV cascades to its owned indexes (the legacy idx_..._ids on mapped_variant_id).
    op.drop_entity(
        PGMaterializedView(schema="public", signature=SIGNATURE, definition=_LEGACY_DEFINITION, with_data=True)
    )
    op.create_entity(
        PGMaterializedView(schema="public", signature=SIGNATURE, definition=_UPGRADE_DEFINITION, with_data=True)
    )
    op.create_index(
        f"idx_{SIGNATURE}_ids",
        SIGNATURE,
        ["mapping_record_id", "variant_id", "score_set_id"],
        unique=True,
    )


def downgrade():
    op.drop_entity(
        PGMaterializedView(schema="public", signature=SIGNATURE, definition=_UPGRADE_DEFINITION, with_data=True)
    )
    op.create_entity(
        PGMaterializedView(schema="public", signature=SIGNATURE, definition=_LEGACY_DEFINITION, with_data=True)
    )
    op.create_index(
        f"idx_{SIGNATURE}_ids",
        SIGNATURE,
        ["mapped_variant_id", "variant_id", "score_set_id"],
        unique=True,
    )
