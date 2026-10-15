from sqlalchemy import join, select

from mavedb.db.view import MaterializedView, view
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

signature = "published_variants_materialized_view"

# One row per (published variant, mapping-record version). The LEFT OUTER JOIN to mapping_records
# carries NO liveness filter on purpose: every record version (live and superseded) is retained so
# the statistics layer can count either the current mappings or the full history via the
# ``current_mapping_record`` flag (``valid_to IS NULL``). Unmapped variants stay in the view with a
# NULL ``mapping_record_id``.
definition = (
    select(
        Variant.id.label("variant_id"),
        Variant.urn.label("variant_urn"),
        MappingRecord.id.label("mapping_record_id"),
        ScoreSet.id.label("score_set_id"),
        ScoreSet.urn.label("score_set_urn"),
        ScoreSet.published_date.label("published_date"),
        MappingRecord.valid_to.is_(None).label("current_mapping_record"),
    )
    .select_from(
        join(Variant, MappingRecord, Variant.id == MappingRecord.variant_id, isouter=True).join(
            ScoreSet, ScoreSet.id == Variant.score_set_id
        )
    )
    .where(
        ScoreSet.published_date.is_not(None),
    )
)


class PublishedVariantsMV(MaterializedView):
    __table__ = view(
        signature,
        definition,
        materialized=True,
    )

    variant_id = __table__.c.variant_id
    variant_urn = __table__.c.variant_urn
    mapping_record_id = __table__.c.mapping_record_id
    score_set_id = __table__.c.score_set_id
    score_set_urn = __table__.c.score_set_urn
    published_date = __table__.c.published_date
    current_mapping_record = __table__.c.current_mapping_record
