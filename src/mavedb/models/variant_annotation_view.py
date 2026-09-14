from sqlalchemy import and_, select

from mavedb.db.base import Base
from mavedb.db.view import view
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

signature = "v_variant_annotations"

# All allele-derived columns are correlated scalar subqueries keyed on the outer MappingRecord, so the
# outer query's FROM stays Variant / ScoreSet / MappingRecord / VariantAnnotationStatus and never
# references Allele / MappingRecordAllele directly. That keeps the grain at one row per
# (variant, annotation_type) — matching the legacy view — and, crucially, avoids ``aliased()`` at
# module import (which would force full mapper configuration before every model is registered, breaking
# a bare ``import`` of this module by Alembic).


def _auth_allele(column):
    """Scalar subquery: ``column`` of the outer record's live **authoritative** (measured) allele.

    Source of the single-valued ``clingen_allele_id``. The authoritative link is 1:1 with the record
    (the mapping job upholds one live authoritative link per record), so this resolves a single allele.
    """
    return (
        select(column)
        .select_from(MappingRecordAllele)
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .where(
            MappingRecordAllele.mapping_record_id == MappingRecord.id,
            MappingRecordAllele.is_authoritative.is_(True),
            MappingRecordAllele.valid_to.is_(None),
        )
        .limit(1)
        .correlate(MappingRecord)
        .scalar_subquery()
    )


def _auth_vep(column):
    """Scalar subquery: ``column`` of the live VEP consequence on the record's authoritative allele."""
    return (
        select(column)
        .select_from(MappingRecordAllele)
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .join(
            VepAlleleConsequence,
            and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.valid_to.is_(None)),
        )
        .where(
            MappingRecordAllele.mapping_record_id == MappingRecord.id,
            MappingRecordAllele.is_authoritative.is_(True),
            MappingRecordAllele.valid_to.is_(None),
        )
        .limit(1)
        .correlate(MappingRecord)
        .scalar_subquery()
    )


def _record_hgvs(level: str, column):
    """Scalar subquery: the canonical HGVS at ``level`` for the outer record.

    Each mapped level (genomic / cdna / protein) is a distinct deduplicated :class:`Allele` linked to
    the record. This constructs a flat ``hgvs_g`` / ``hgvs_c`` / ``hgvs_p`` triple by pivoting the record's
    live allele links by level — yielding ``NULL`` where the record has no allele at that level.
    """
    return (
        select(column)
        .select_from(MappingRecordAllele)
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .where(
            MappingRecordAllele.mapping_record_id == MappingRecord.id,
            MappingRecordAllele.valid_to.is_(None),
            Allele.level == level,
        )
        .limit(1)
        .correlate(MappingRecord)
        .scalar_subquery()
    )


# Flat operator/CLI convenience view: one row per (variant, current annotation_type).
definition = (
    select(
        Variant.urn.label("variant_urn"),
        ScoreSet.urn.label("score_set_urn"),
        Variant.hgvs_nt,
        Variant.hgvs_pro,
        Variant.hgvs_splice,
        MappingRecord.id.label("mapping_record_id"),
        _auth_allele(Allele.clingen_allele_id).label("clingen_allele_id"),
        MappingRecord.hgvs_assay_level,
        _record_hgvs("genomic", Allele.hgvs_g).label("hgvs_g"),
        _record_hgvs("cdna", Allele.hgvs_c).label("hgvs_c"),
        _record_hgvs("protein", Allele.hgvs_p).label("hgvs_p"),
        _auth_vep(VepAlleleConsequence.functional_consequence).label("vep_functional_consequence"),
        _auth_vep(VepAlleleConsequence.access_date).label("vep_access_date"),
        MappingRecord.mapped_date,
        MappingRecord.mapping_api_version,
        MappingRecord.vrs_version,
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
    # The variant's live mapping record. live-ness rides the ON clause (never a WHERE) so the outer
    # join is preserved for unmapped variants — a WHERE would reject the null-record row.
    .outerjoin(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)))
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
    mapping_record_id = __table__.c.mapping_record_id
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
    annotation_type = __table__.c.annotation_type
    annotation_status = __table__.c.annotation_status
    failure_category = __table__.c.failure_category
    annotation_error = __table__.c.annotation_error
    annotation_version = __table__.c.annotation_version
    annotation_metadata = __table__.c.annotation_metadata
    annotation_created_at = __table__.c.annotation_created_at
    annotation_updated_at = __table__.c.annotation_updated_at
