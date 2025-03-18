from sqlalchemy import select, join

from mavedb.db.view import MaterializedView, view

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant


signature = "published_variants_materialized_view"
definition = (
    select(
        Variant.id.label("variant_id"),
        Variant.urn.label("variant_urn"),
        MappedVariant.id.label("mapped_variant_id"),
        ScoreSet.id.label("score_set_id"),
        ScoreSet.urn.label("score_set_urn"),
        ScoreSet.published_date.label("published_date"),
        MappedVariant.current.label("current_mapped_variant"),
    )
    .select_from(
        join(Variant, MappedVariant, Variant.id == MappedVariant.variant_id, isouter=True).join(
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
    mapped_variant_id = __table__.c.mapped_variant_id
    score_set_id = __table__.c.score_set_id
    score_set_urn = __table__.c.score_set_urn
    published_date = __table__.c.published_date
    current_mapped_variant = __table__.c.current_mapped_variant
