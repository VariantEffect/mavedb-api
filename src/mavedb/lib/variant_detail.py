"""Assembles the variant-detail envelope backing ``GET /variants/{urn}``.

The envelope has two tiers: flat, UI-ergonomic assay-level fields (HGVS pair, digest, ClinGen id),
and a spec-pure GA4GH ``CategoricalVariant`` (built by :mod:`lib.cat_vrs`) with a MaveDB layer
riding alongside it, keyed by VRS digest — per-allele identity (level, HGVS, ClinGen id,
member→defining relation) and external annotations (:mod:`lib.allele_annotations`). Also includes
the variant's functional ``classifications`` per calibration, and its version standing
(``is_current`` / ``superseded_by_score_set``), so a superseded variant self-describes.

``as_of`` only reconstructs the **molecular** layer (Cat-VRS membership and VEP/gnomAD/ClinVar
annotations) at the past instant. Scores are immutable and calibrations carry no ``ValidTime``, so
both are always returned as they stand now, regardless of ``as_of``.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleAnnotations, get_allele_annotations
from mavedb.lib.allele_identity import AlleleDerivation, AlleleIdentity
from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.cat_vrs import categorical_variant_for_variant, is_convergent_encoding
from mavedb.lib.score_calibrations import calibration_preference_key
from mavedb.models.enums.sequence_level import SequenceLevel
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
    """One functional classification the variant belongs to, tagged with its calibration.

    ``calibration_id`` / ``primary`` locate it among the score set's calibrations (``primary`` is
    the UI default). ``classification`` is the ORM
    :class:`ScoreCalibrationFunctionalClassification`, serialized at the view-model boundary.
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
    assay_level: Optional[SequenceLevel]
    target_hgvs: Optional[str]  # submitted, target/assay coordinates
    reference_hgvs: Optional[str]  # mapped, reference coordinates, assay level
    assay_level_digest: Optional[str]
    clingen_allele_id: Optional[str]

    # Raw GA4GH VRS pair, surfaced flat so a bulk/VRS consumer doesn't need to dig the measured
    # allele out of the Cat-VRS below.
    pre_mapped: Optional[dict[str, Any]]
    post_mapped: Optional[dict[str, Any]]

    # Spec-pure GA4GH Cat-VRS (no MaveDB fields), plus the MaveDB layer alongside it: a per-allele
    # identity sidecar keyed by VRS digest, one entry per linked allele (shares keys with
    # `annotations`), carrying level, HGVS, ClinGen id, and relation.
    molecular_representation: Optional[dict[str, Any]]
    mode: Optional[str]  # CatVrsMode value — projection | reverse_translation
    alleles: dict[str, AlleleIdentity]

    # External annotations, keyed by VRS digest, joined to the Cat-VRS members / the alleles sidecar.
    annotations: dict[str, AlleleAnnotations]

    # Version standing — self-descriptive for a superseded variant.
    is_current: bool
    # Score-set URN (not variant URN) of the version that supersedes this one, if any and readable.
    # Supersession is versioned at the score-set level, and a newer version may add/drop/renumber
    # variants, so consumers must look this variant up within that score set rather than follow a
    # superseding-variant pointer.
    superseded_by_score_set: Optional[str]


def _classifications_for_variant(
    db: Session, variant: Variant, *, visible_calibration_ids: Optional[set[int]]
) -> list[VariantClassificationRecord]:
    """Functional classifications the variant belongs to, one row per calibration that classifies it.

    Membership is read from the calibration→classification→variant association table, ordered by
    ``calibration_preference_key`` (+ id) so the first entry matches the UI's default.
    ``visible_calibration_ids`` restricts to readable calibrations (``None`` = no restriction, for
    lib-level callers).
    """
    statement = (
        select(ScoreCalibrationFunctionalClassification, ScoreCalibration)
        .join(
            classification_variants,
            classification_variants.c.functional_classification_id == ScoreCalibrationFunctionalClassification.id,
        )
        .join(ScoreCalibration, ScoreCalibration.id == ScoreCalibrationFunctionalClassification.calibration_id)
        .where(classification_variants.c.variant_id == variant.id)
    )
    if visible_calibration_ids is not None:
        statement = statement.where(ScoreCalibration.id.in_(visible_calibration_ids))

    rows = sorted(
        db.execute(statement).tuples().all(), key=lambda row: (*calibration_preference_key(row[1]), row[1].id)
    )
    return [
        VariantClassificationRecord(
            calibration_id=calibration.id, primary=calibration.primary, classification=classification
        )
        for classification, calibration in rows
    ]


def _derivation_for(*, assay_level: Optional[SequenceLevel], is_convergent: bool) -> AlleleDerivation:
    """Provenance of a *non-focus* linked allele, derived from the assay level.

    The measured allele is the focus (no derivation). For every other allele:

    - **Protein assay** → ``CANDIDATE``: reverse translation is ambiguous, so the derived nucleotide
      fan-out is only a candidate (which codon was actually measured is unknown).
    - **Nucleotide assay, convergent encoding** (``is_convergent``) → ``CONVERGENT``: an nt member in
      a different projection group. A distinct, precisely-known variant that happens to converge on
      the same protein consequence, not a projection of the measured change.
    - **Nucleotide assay, otherwise** → ``PROJECTION``: the deterministic class derived from the
      measured change itself.

    ``is_convergent`` comes from :func:`cat_vrs.is_convergent_encoding`, keeping this ``derivation``
    axis aligned with the Cat-VRS ``relation`` axis (``co_encodes`` ↔ ``convergent``,
    ``coordinate_representation_of`` / ``translation_of`` ↔ ``projection``, ``encodes`` ↔
    ``candidate``).
    """
    if assay_level == SequenceLevel.protein.value:
        return AlleleDerivation.CANDIDATE
    if is_convergent:
        return AlleleDerivation.CONVERGENT
    return AlleleDerivation.PROJECTION


def _submitted_assay_level_hgvs(variant: Variant, assay_level: Optional[SequenceLevel]) -> Optional[str]:
    """Depositor-submitted HGVS in the variant's assay frame: ``hgvs_pro`` for a protein assay,
    otherwise ``hgvs_nt`` (genomic and coding assays share this column; ``hgvs_splice`` is never an
    index column)."""
    if assay_level == SequenceLevel.protein.value:
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

    ``superseding_score_set`` is the newer version the caller already resolved for visibility
    (``None`` if unreadable, mirroring ``fetch_score_set_by_urn``); its presence drives
    ``is_current`` / ``superseded_by_score_set``. ``visible_calibration_ids`` restricts
    classifications to readable calibrations. ``as_of`` reconstructs the molecular layer only —
    see the module docstring.
    """
    data = variant.data if isinstance(variant.data, dict) else {}
    scores = data.get("score_data")
    counts = data.get("count_data")

    # The live mapping record supplies the assay level and the mapped (reference-frame) assay HGVS.
    record = db.scalar(
        select(MappingRecord).where(MappingRecord.variant_id == variant.id).where(MappingRecord.live_at(as_of))
    )
    assay_level = SequenceLevel(record.assay_level) if record is not None else None
    reference_hgvs = record.hgvs_assay_level if record is not None else None

    # The live allele links: the authoritative allele gives the assay-level digest + ClinGen id; all
    # linked alleles seed the digest-keyed annotation map.
    links = get_live_record_allele_links(db, variant.id, as_of=as_of)
    authoritative = next((link for link in links if link.is_authoritative), None)
    assay_level_digest = authoritative.allele.vrs_digest if authoritative is not None else None
    clingen_allele_id = authoritative.allele.clingen_allele_id if authoritative is not None else None

    annotations = get_allele_annotations(db, [link.allele for link in links], as_of=as_of)

    # Spec-pure Cat-VRS built on the fly, plus mode + per-member relations. The defining allele is
    # deliberately absent from `relations`, so it gets relation=None below.
    transit = categorical_variant_for_variant(db, variant.id, name=variant.urn or "", as_of=as_of)
    if transit is not None:
        molecular_representation = transit.categorical_variant.model_dump(mode="json", exclude_none=True)
        mode: Optional[str] = transit.mode.value
        relations = transit.member_relations
    else:
        molecular_representation = None
        mode = None
        relations = {}

    # Per-allele identity sidecar: one entry per linked allele, keyed by VRS digest (the same link
    # set that seeds `annotations`, so the two maps share keys). Carries level, reference-frame HGVS
    # (exactly one of hgvs_g/c/p is populated per allele), ClinGen id, and the member→defining relation.

    # projection_group -> the digests sharing it, for within-record projection grouping. A link's
    # projection is the ≤1 *other* digest in its group; the protein apex and any pre-RT link carry a
    # NULL group, so they appear in no bucket and projection_of stays None for them.
    group_digests: dict[int, list[str]] = {}
    for link in links:
        if link.projection_group is not None and link.allele.vrs_digest is not None:
            group_digests.setdefault(link.projection_group, []).append(link.allele.vrs_digest)

    # Anchor for the convergence test: an nt member in a *different* projection group than the
    # measured change is a convergent encoding (co_encodes / convergent), not a projection of it.
    defining_group = authoritative.projection_group if authoritative is not None else None

    alleles: dict[str, AlleleIdentity] = {}
    for link in links:
        allele = link.allele

        # The other digest in this link's projection_group, if any (groups have ≤2 members).
        projection_of: Optional[str] = None
        if link.projection_group is not None:
            projection_of = next(
                (digest for digest in group_digests.get(link.projection_group, []) if digest != allele.vrs_digest),
                None,
            )

        relation = relations.get(allele.vrs_digest)
        is_convergent = is_convergent_encoding(allele.level, link.projection_group, defining_group=defining_group)
        alleles[allele.vrs_digest] = AlleleIdentity(
            level=allele.level,
            hgvs=allele.hgvs_g or allele.hgvs_c or allele.hgvs_p,
            clingen_allele_id=allele.clingen_allele_id,
            is_focus=link.is_authoritative,
            relation=relation.value if relation is not None else None,
            derivation=(
                None if link.is_authoritative else _derivation_for(assay_level=assay_level, is_convergent=is_convergent)
            ),
            projection_of=projection_of,
        )

    return VariantDetail(
        # TODO(#372)
        urn=variant.urn or "",
        scores=scores,
        counts=counts,
        classifications=_classifications_for_variant(db, variant, visible_calibration_ids=visible_calibration_ids),
        assay_level=assay_level,
        target_hgvs=_submitted_assay_level_hgvs(variant, assay_level),
        reference_hgvs=reference_hgvs,
        assay_level_digest=assay_level_digest,
        clingen_allele_id=clingen_allele_id,
        pre_mapped=record.pre_mapped if record is not None else None,
        post_mapped=authoritative.allele.post_mapped if authoritative is not None else None,
        molecular_representation=molecular_representation,
        mode=mode,
        alleles=alleles,
        annotations=annotations,
        is_current=superseding_score_set is None,
        superseded_by_score_set=superseding_score_set.urn if superseding_score_set is not None else None,
    )
