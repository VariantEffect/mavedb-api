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
from typing import Any, Optional, Sequence

from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleAnnotations, get_allele_annotations
from mavedb.lib.allele_identity import AlleleIdentity
from mavedb.lib.alleles import LiveRecordLinks, get_live_record_allele_links
from mavedb.lib.cat_vrs import build_categorical_variant, is_projection_partner, member_label
from mavedb.lib.score_calibrations import calibration_preference_key, classifications_by_variant
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
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
class VariantDetailInputs:
    """The database inputs for a chunk of variants' detail envelopes, loaded up front.

    ``annotations`` is keyed by digest across the whole chunk; each envelope takes the entries for its own
    links. :meth:`detail_for` assembles one variant's envelope without touching the database.
    """

    record_links: dict[int, LiveRecordLinks]
    annotations: dict[str, AlleleAnnotations]
    classifications: dict[int, list[VariantClassificationRecord]]

    def detail_for(self, variant: Variant, *, superseding_score_set: Optional[ScoreSet] = None) -> "VariantDetail":
        """Assemble the variant-detail envelope for ``variant``.

        ``superseding_score_set`` is the newer version the caller already resolved for visibility
        (``None`` if unreadable, mirroring ``fetch_score_set_by_urn``); its presence drives
        ``is_current`` / ``superseded_by_score_set``.
        """
        return _assemble_variant_detail(
            variant,
            self.record_links.get(variant.id),
            self.annotations,
            self.classifications.get(variant.id, []),
            superseding_score_set=superseding_score_set,
        )


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


def _classifications_by_variant(
    db: Session, variant_ids: Sequence[int], *, visible_calibration_ids: Optional[set[int]]
) -> dict[int, list[VariantClassificationRecord]]:
    """Functional classifications each variant belongs to, one row per calibration that classifies it.

    Each variant's rows are ordered by ``calibration_preference_key`` (+ id), so the first entry matches the
    UI's default. ``visible_calibration_ids`` restricts to readable calibrations (``None`` = no restriction,
    for lib-level callers).
    """
    return {
        variant_id: [
            VariantClassificationRecord(
                calibration_id=calibration.id, primary=calibration.primary, classification=classification
            )
            for classification, calibration in sorted(
                pairs, key=lambda pair: (*calibration_preference_key(pair[1]), pair[1].id)
            )
            if visible_calibration_ids is None or calibration.id in visible_calibration_ids
        ]
        for variant_id, pairs in classifications_by_variant(db, variant_ids).items()
    }


def _submitted_assay_level_hgvs(variant: Variant, assay_level: Optional[SequenceLevel]) -> Optional[str]:
    """Depositor-submitted HGVS in the variant's assay frame: ``hgvs_pro`` for a protein assay,
    otherwise ``hgvs_nt`` (genomic and coding assays share this column; ``hgvs_splice`` is never an
    index column)."""
    if assay_level == SequenceLevel.protein.value:
        return variant.hgvs_pro
    return variant.hgvs_nt


def load_variant_detail_inputs(
    db: Session,
    variants: Sequence[Variant],
    *,
    visible_calibration_ids: Optional[set[int]] = None,
    as_of: Optional[datetime] = None,
) -> VariantDetailInputs:
    """Load the detail-envelope inputs for ``variants`` in six queries in total, however many variants there
    are (where building each envelope alone would cost six per variant).

    Two for the records and links, three for the annotations of every linked allele, and one for the
    classifications. Whole-set callers pass a chunk of variants (:data:`~mavedb.lib.alleles.VARIANT_CHUNK_SIZE`).
    ``visible_calibration_ids`` restricts classifications to readable calibrations. ``as_of`` reconstructs
    the molecular layer only — see the module docstring.
    """
    variant_ids = [variant.id for variant in variants]
    record_links = get_live_record_allele_links(db, variant_ids, as_of=as_of)
    alleles = [link.allele for entry in record_links.values() for link in entry.links]
    return VariantDetailInputs(
        record_links=record_links,
        annotations=get_allele_annotations(db, alleles, as_of=as_of),
        classifications=_classifications_by_variant(db, variant_ids, visible_calibration_ids=visible_calibration_ids),
    )


def get_variant_detail(
    db: Session,
    variant: Variant,
    *,
    superseding_score_set: Optional[ScoreSet] = None,
    visible_calibration_ids: Optional[set[int]] = None,
    as_of: Optional[datetime] = None,
) -> VariantDetail:
    """Assemble the variant-detail envelope for one variant: the single-variant form of
    :func:`load_variant_detail_inputs` and :meth:`VariantDetailInputs.detail_for`."""
    inputs = load_variant_detail_inputs(db, [variant], visible_calibration_ids=visible_calibration_ids, as_of=as_of)
    return inputs.detail_for(variant, superseding_score_set=superseding_score_set)


def _assemble_variant_detail(
    variant: Variant,
    record_links: Optional[LiveRecordLinks],
    chunk_annotations: dict[str, AlleleAnnotations],
    classifications: list[VariantClassificationRecord],
    *,
    superseding_score_set: Optional[ScoreSet],
) -> VariantDetail:
    """Build one variant's envelope from its loaded record links, the chunk's annotations and its
    classifications."""
    data = variant.data if isinstance(variant.data, dict) else {}
    scores = data.get("score_data")
    counts = data.get("count_data")

    # The live mapping record supplies the assay level and the mapped (reference-frame) assay HGVS.
    record = record_links.record if record_links is not None else None
    assay_level = SequenceLevel(record.assay_level) if record is not None else None
    reference_hgvs = record.hgvs_assay_level if record is not None else None

    # The live allele links: the authoritative allele gives the assay-level digest + ClinGen id; all
    # linked alleles seed the digest-keyed annotation map.
    links = record_links.links if record_links is not None else []
    authoritative = next((link for link in links if link.is_authoritative), None)
    assay_level_digest = authoritative.allele.vrs_digest if authoritative is not None else None
    clingen_allele_id = authoritative.allele.clingen_allele_id if authoritative is not None else None

    annotations = {
        link.allele.vrs_digest: chunk_annotations[link.allele.vrs_digest]
        for link in links
        if link.allele.vrs_digest in chunk_annotations
    }

    # Spec-pure Cat-VRS built on the fly, plus its mode.
    transit = build_categorical_variant(
        links,
        name=variant.urn or "",
        cross_references={digest: entry.cross_references() for digest, entry in annotations.items()},
    )
    if transit is not None:
        molecular_representation = transit.categorical_variant.model_dump(mode="json", exclude_none=True)
        mode: Optional[str] = transit.mode.value
    else:
        molecular_representation = None
        mode = None

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

    # Members are labelled relative to the measured (authoritative) allele.
    focus_level = authoritative.allele.level if authoritative is not None else None
    focus_group = authoritative.projection_group if authoritative is not None else None

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

        relation, derivation = (
            (None, None)
            if link.is_authoritative
            else member_label(
                focus_level,
                allele.level,
                is_projection=is_projection_partner(link.projection_group, focus_group=focus_group),
            )
        )
        alleles[allele.vrs_digest] = AlleleIdentity(
            level=allele.level,
            hgvs=allele.hgvs,
            clingen_allele_id=allele.clingen_allele_id,
            is_focus=link.is_authoritative,
            relation=relation.value if relation is not None else None,
            derivation=derivation,
            projection_of=projection_of,
        )

    return VariantDetail(
        # TODO(#372)
        urn=variant.urn or "",
        scores=scores,
        counts=counts,
        classifications=classifications,
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
