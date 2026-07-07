"""The assayed variant-detail view backing ``GET /variants/{urn}``.

Assembles the two-tier envelope for a single variant: flat, UI-ergonomic assay fields (the
assay-level HGVS pair, digest, ClinGen id) plus the spec-pure GA4GH ``CategoricalVariant`` (built
on the fly by :mod:`lib.cat_vrs`) and, riding alongside it keyed by VRS digest, the MaveDB layer —
per-member relations and the digest-keyed external-annotation map (:mod:`lib.annotations`). Also
carries the per-calibration functional ``classifications`` the variant falls into, and its version
standing (``is_current`` / ``superseded_by_score_set``) so a superseded variant self-describes rather
than reading as current (design §9.2).

Temporal scope of ``as_of``: only the **molecular** layer is versioned — Cat-VRS membership and the
VEP/gnomAD/ClinVar annotations reconstruct at the past instant. Scores are immutable (``Variant`` is
not ``ValidTime``) and calibrations carry no ``ValidTime`` at all (they are frozen/append-only), so
scores and classifications are as-of-invariant and returned as they stand now. ``as_of`` defaults to
the currently-live rows.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleAnnotations, get_allele_annotations
from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.cat_vrs import categorical_variant_for_variant
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table as classification_variants,
)
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant


@dataclass(frozen=True)
class VariantClassificationRecord:
    """One functional classification the variant falls into, tagged with its calibration context.

    ``calibration_id`` / ``primary`` locate the classification among the score set's (possibly
    several) calibrations — ``primary`` is the UI default. ``classification`` is the ORM
    :class:`ScoreCalibrationFunctionalClassification` (functional call, ACMG, oddspath), serialized
    at the view-model boundary.
    """

    calibration_id: int
    primary: bool
    classification: ScoreCalibrationFunctionalClassification


@dataclass(frozen=True)
class VariantDetail:
    """The assembled variant-detail envelope (transit; serialized by ``view_models.variant``)."""

    urn: str
    scores: Optional[dict[str, Any]]
    counts: Optional[dict[str, Any]]
    classifications: list[VariantClassificationRecord]

    # Flat, UI-ergonomic assay-level fields.
    assay_level: Optional[str]
    target_hgvs: Optional[str]  # submitted, target/assay coordinates
    reference_hgvs: Optional[str]  # mapped, reference coordinates, assay level
    assay_level_digest: Optional[str]
    clingen_allele_id: Optional[str]

    # Spec-pure GA4GH Cat-VRS (no MaveDB fields inside), plus the MaveDB layer riding alongside.
    molecular_representation: Optional[dict[str, Any]]
    mode: Optional[str]  # CatVrsMode value — projection | reverse_translation
    member_relations: dict[str, str]  # member vrs_digest -> relation (member -> defining)

    # External annotations, keyed by VRS digest, joined to the Cat-VRS members.
    annotations: dict[str, AlleleAnnotations]

    # Version standing — self-descriptive for a superseded variant.
    is_current: bool
    # URN of the score-set version that supersedes this variant's, if any (and readable). This is a
    # *score set* URN, not a variant URN — supersession is versioned at the score-set level, and a newer
    # version may add/drop/renumber variants, so there is no stable superseding-variant to point at;
    # consumers resolve the current measurement by looking this variant up within that score set.
    superseded_by_score_set: Optional[str]


def _classifications_for_variant(
    db: Session, variant: Variant, *, visible_calibration_ids: Optional[set[int]]
) -> list[VariantClassificationRecord]:
    """The functional classifications the variant belongs to (membership is materialized on the
    calibration→classification→variant m2m). One row per calibration that classifies the variant;
    the primary calibration sorts first. ``visible_calibration_ids`` restricts to calibrations the
    caller resolved as readable (``None`` = no restriction, for lib-level use)."""
    statement = (
        select(ScoreCalibrationFunctionalClassification, ScoreCalibration.primary, ScoreCalibration.id)
        .join(
            classification_variants,
            classification_variants.c.functional_classification_id == ScoreCalibrationFunctionalClassification.id,
        )
        .join(ScoreCalibration, ScoreCalibration.id == ScoreCalibrationFunctionalClassification.calibration_id)
        .where(classification_variants.c.variant_id == variant.id)
        .order_by(ScoreCalibration.primary.desc(), ScoreCalibration.id)
    )
    if visible_calibration_ids is not None:
        statement = statement.where(ScoreCalibration.id.in_(visible_calibration_ids))

    return [
        VariantClassificationRecord(calibration_id=calibration_id, primary=primary, classification=classification)
        for classification, primary, calibration_id in db.execute(statement)
    ]


def _submitted_assay_level_hgvs(variant: Variant, assay_level: Optional[str]) -> Optional[str]:
    """The depositor-submitted HGVS in the variant's assay frame: protein for a protein assay,
    otherwise the nucleotide expression (genomic or coding share ``hgvs_nt``)."""
    if assay_level == AnnotationLayer.protein.value:
        return variant.hgvs_pro
    return variant.hgvs_nt


def get_variant_detail(
    db: Session,
    variant: Variant,
    *,
    superseding_score_set: Optional[ScoreSet] = None,
    visible_calibration_ids: Optional[set[int]] = None,
    as_of: Optional[datetime] = None,
) -> VariantDetail:
    """Assemble the variant-detail envelope for ``variant``.

    ``superseding_score_set`` is the newer version the caller has already resolved for visibility
    (blanked when the user cannot read it, mirroring ``fetch_score_set_by_urn``); its presence drives
    ``is_current``/``superseded_by_score_set``. ``visible_calibration_ids`` restricts classifications to
    readable calibrations. ``as_of`` reconstructs the molecular layer (Cat-VRS membership +
    annotations); scores and classifications are as-of-invariant. See the module docstring.
    """
    data = variant.data if isinstance(variant.data, dict) else {}
    scores = data.get("score_data")
    counts = data.get("count_data")

    # The live mapping record supplies the assay level and the mapped (reference-frame) assay HGVS.
    record = db.scalar(
        select(MappingRecord).where(MappingRecord.variant_id == variant.id).where(MappingRecord.live_at(as_of))
    )
    assay_level = record.assay_level if record is not None else None
    reference_hgvs = record.hgvs_assay_level if record is not None else None

    # The live allele links: the authoritative allele gives the assay-level digest + ClinGen id; all
    # linked alleles seed the digest-keyed annotation map.
    links = get_live_record_allele_links(db, variant.id, as_of=as_of)
    authoritative = next((link for link in links if link.is_authoritative), None)
    assay_level_digest = authoritative.allele.vrs_digest if authoritative is not None else None
    clingen_allele_id = authoritative.allele.clingen_allele_id if authoritative is not None else None

    annotations = get_allele_annotations(db, [link.allele for link in links], as_of=as_of)

    # Spec-pure Cat-VRS built on the fly, plus the MaveDB layer (mode + per-member relations).
    transit = categorical_variant_for_variant(db, variant.id, name=variant.urn or "", as_of=as_of)
    if transit is not None:
        molecular_representation = transit.categorical_variant.model_dump(mode="json", exclude_none=True)
        mode: Optional[str] = transit.mode.value
        member_relations = {digest: relation.value for digest, relation in transit.member_relations.items()}
    else:
        molecular_representation = None
        mode = None
        member_relations = {}

    return VariantDetail(
        urn=variant.urn or "",
        scores=scores,
        counts=counts,
        classifications=_classifications_for_variant(db, variant, visible_calibration_ids=visible_calibration_ids),
        assay_level=assay_level,
        target_hgvs=_submitted_assay_level_hgvs(variant, assay_level),
        reference_hgvs=reference_hgvs,
        assay_level_digest=assay_level_digest,
        clingen_allele_id=clingen_allele_id,
        molecular_representation=molecular_representation,
        mode=mode,
        member_relations=member_relations,
        annotations=annotations,
        is_current=superseding_score_set is None,
        superseded_by_score_set=superseding_score_set.urn if superseding_score_set is not None else None,
    )
