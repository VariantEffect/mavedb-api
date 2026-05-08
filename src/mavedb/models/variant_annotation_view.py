from sqlalchemy import and_, select

from mavedb.db.base import Base
from mavedb.db.view import view
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus

signature = "v_variant_annotations"

definition = (
    select(
        Variant.urn.label("variant_urn"),
        ScoreSet.urn.label("score_set_urn"),
        Variant.hgvs_nt,
        Variant.hgvs_pro,
        Variant.hgvs_splice,
        MappedVariant.id.label("mapped_variant_id"),
        MappedVariant.clingen_allele_id,
        MappedVariant.hgvs_assay_level,
        MappedVariant.hgvs_g,
        MappedVariant.hgvs_c,
        MappedVariant.hgvs_p,
        MappedVariant.vep_functional_consequence,
        MappedVariant.vep_access_date,
        MappedVariant.mapped_date,
        MappedVariant.mapping_api_version,
        MappedVariant.vrs_version,
        MappedVariant.error_message.label("mapping_error"),
        VariantAnnotationStatus.annotation_type,
        VariantAnnotationStatus.status.label("annotation_status"),
        VariantAnnotationStatus.failure_category,
        VariantAnnotationStatus.error_message.label("annotation_error"),
        VariantAnnotationStatus.version.label("annotation_version"),
        VariantAnnotationStatus.annotation_metadata,
        VariantAnnotationStatus.current,
        VariantAnnotationStatus.created_at.label("annotation_created_at"),
        VariantAnnotationStatus.updated_at.label("annotation_updated_at"),
    )
    .select_from(Variant)
    .join(ScoreSet, ScoreSet.id == Variant.score_set_id)
    .outerjoin(
        MappedVariant,
        and_(MappedVariant.variant_id == Variant.id, MappedVariant.current == True),  # noqa: E712
    )
    .outerjoin(
        VariantAnnotationStatus,
        and_(VariantAnnotationStatus.variant_id == Variant.id, VariantAnnotationStatus.current == True),  # noqa: E712
    )
)


class VariantAnnotationView(Base):
    __table__ = view(signature, definition, materialized=False)

    variant_urn = __table__.c.variant_urn
    score_set_urn = __table__.c.score_set_urn
    hgvs_nt = __table__.c.hgvs_nt
    hgvs_pro = __table__.c.hgvs_pro
    hgvs_splice = __table__.c.hgvs_splice
    mapped_variant_id = __table__.c.mapped_variant_id
    clingen_allele_id = __table__.c.clingen_allele_id
    hgvs_assay_level = __table__.c.hgvs_assay_level
    hgvs_g = __table__.c.hgvs_g
    hgvs_c = __table__.c.hgvs_c
    hgvs_p = __table__.c.hgvs_p
    vep_functional_consequence = __table__.c.vep_functional_consequence
    vep_access_date = __table__.c.vep_access_date
    mapped_date = __table__.c.mapped_date
    mapping_api_version = __table__.c.mapping_api_version
    vrs_version = __table__.c.vrs_version
    mapping_error = __table__.c.mapping_error
    annotation_type = __table__.c.annotation_type
    annotation_status = __table__.c.annotation_status
    failure_category = __table__.c.failure_category
    annotation_error = __table__.c.annotation_error
    annotation_version = __table__.c.annotation_version
    annotation_metadata = __table__.c.annotation_metadata
    annotation_created_at = __table__.c.annotation_created_at
    annotation_updated_at = __table__.c.annotation_updated_at
