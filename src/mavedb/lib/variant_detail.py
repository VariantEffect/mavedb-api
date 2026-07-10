"""The assayed variant-detail view backing ``GET /variants/{urn}``.

Assembles the two-tier envelope for a single variant: flat, UI-ergonomic assay fields (the
assay-level HGVS pair, digest, ClinGen id) plus the spec-pure GA4GH ``CategoricalVariant`` (built
on the fly by :mod:`lib.cat_vrs`) and, riding alongside it keyed by VRS digest, the MaveDB layer —
the per-allele identity sidecar (level / HGVS / ClinGen id / member→defining relation) and the
digest-keyed external-annotation map (:mod:`lib.allele_annotations`). Also
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
from enum import Enum
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.allele_annotations import AlleleAnnotations, get_allele_annotations
from mavedb.lib.alleles import get_live_record_allele_links
from mavedb.lib.cat_vrs import categorical_variant_for_variant
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
    """One functional classification the variant falls into, tagged with its calibration context.

    ``calibration_id`` / ``primary`` locate the classification among the score set's (possibly
    several) calibrations — ``primary`` is the UI default. ``classification`` is the ORM
    :class:`ScoreCalibrationFunctionalClassification` (functional call, ACMG, oddspath), serialized
    at the view-model boundary.
    """

    calibration_id: int
    primary: bool
    classification: ScoreCalibrationFunctionalClassification


class AlleleDerivation(str, Enum):
    """How an allele's representation was arrived at — its confidence/provenance axis.

    Distinct from (orthogonal to) the Cat-VRS ``relation`` axis: ``relation`` is *structural* (which
    level relates to which, member→defining), whereas ``derivation`` is *epistemic* (how much to trust
    the representation). See the design's "Semantics note — two axes, keep them separate". Derived at
    serialization time from ``is_authoritative`` + the assay level — no stored column.
    """

    # The assay's actual measurement (the authoritative link). Precise by definition.
    AUTHORITATIVE = "authoritative"
    # Deterministic and precise. Every allele derived from a *nucleotide* measurement is a projection:
    # nucleotide↔nucleotide and nucleotide→protein are both deterministic given (assembly, transcript).
    PROJECTION = "projection"
    # Reverse-translation output of a *protein* measurement — genuinely ambiguous (many synonymous
    # codons). One member of the fanned-out equivalence class, not a precise coordinate.
    CANDIDATE = "candidate"


@dataclass(frozen=True)
class AlleleIdentity:
    """The MaveDB molecular-identity facts for one of the variant's linked alleles.

    Rides alongside the spec-pure Cat-VRS keyed by VRS digest (the ``alleles`` map). ``level`` and
    ``hgvs`` (the reference-frame HGVS, exactly one of the allele's genomic/coding/protein columns) are
    what the UI labels the per-level annotation panel by — *never* the digest.

    Three axes ride here, deliberately independent:

    - ``relation`` (Cat-VRS, structural): this allele's member→defining relation
      (``is_protein_of`` / ``coordinate_representation_of`` / …). ``None`` when it *is* the measured
      allele, or when the allele is not a Cat-VRS member. Sourced from
      ``cat_vrs.CategoricalVariantTransit.member_relations``.
    - ``derivation`` (provenance): :class:`AlleleDerivation` — authoritative / projection /
      candidate. Orthogonal to ``relation``; never conflate the two (a protein member of a nucleotide
      assay has ``relation=translation_of`` but ``derivation=projection``, whereas the same-shaped
      member of a protein assay is ``derivation=candidate``).
    - ``projection_of`` (provenance): the VRS digest of this allele's projection sibling — the ≤1 *other*
      member of its ``projection_group`` (a c↔g pair). ``None`` for the protein apex (group ``NULL``),
      for pre-reverse-translation data, and where a level's projection failed.
    """

    level: Optional[str]
    hgvs: Optional[str]
    clingen_allele_id: Optional[str]
    relation: Optional[str]
    derivation: Optional[str]
    projection_of: Optional[str]


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

    # Spec-pure GA4GH Cat-VRS (no MaveDB fields inside), plus the MaveDB layer riding alongside: the
    # per-allele identity sidecar, keyed by VRS digest — one entry per linked allele (the record-scoped,
    # all-levels set, sharing keys with `annotations`), carrying level + HGVS + ClinGen id + relation.
    molecular_representation: Optional[dict[str, Any]]
    mode: Optional[str]  # CatVrsMode value — projection | reverse_translation
    alleles: dict[str, AlleleIdentity]

    # External annotations, keyed by VRS digest, joined to the Cat-VRS members / the alleles sidecar.
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
    calibration→classification→variant m2m). One row per calibration that classifies the variant, ordered
    by the shared calibration preference cascade (``calibration_preference_key`` + id) so the first entry
    is the same default the UI and the allele-measurements card surface. ``visible_calibration_ids``
    restricts to calibrations the caller resolved as readable (``None`` = no restriction, for lib-level use)."""
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


def _derivation_for(*, is_authoritative: bool, assay_level: Optional[SequenceLevel]) -> AlleleDerivation:
    """The provenance of a linked allele's representation, from ``is_authoritative`` + the assay level.

    The measured allele is ``authoritative``. Every *other* allele's confidence is set by the *only*
    ambiguous boundary in the stack — protein → codon:

    - **nucleotide assay** (``assay_level`` genomic/cdna) → ``projection`` for all derived alleles.
      nucleotide↔nucleotide and nucleotide→protein are deterministic, so the whole equivalence class
      is precise.
    - **protein assay** (``assay_level`` protein) → ``candidate`` for the derived (nucleotide) fan-out.
      Reverse translation is genuinely ambiguous.

    Only ``assay_level`` (not the allele's own level) distinguishes the two, because on a nucleotide
    assay every derived level is deterministic and on a protein assay every derived allele is part of
    the ambiguous nucleotide fan-out.
    """
    if is_authoritative:
        return AlleleDerivation.AUTHORITATIVE
    if assay_level == SequenceLevel.protein.value:
        return AlleleDerivation.CANDIDATE
    return AlleleDerivation.PROJECTION


def _submitted_assay_level_hgvs(variant: Variant, assay_level: Optional[SequenceLevel]) -> Optional[str]:
    """The depositor-submitted HGVS in the variant's assay frame: protein for a protein assay,
    otherwise the nucleotide expression (genomic or coding share ``hgvs_nt`` and ``hgvs_splice`` is
    never an index column)."""
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
    assay_level = SequenceLevel(record.assay_level) if record is not None else None
    reference_hgvs = record.hgvs_assay_level if record is not None else None

    # The live allele links: the authoritative allele gives the assay-level digest + ClinGen id; all
    # linked alleles seed the digest-keyed annotation map.
    links = get_live_record_allele_links(db, variant.id, as_of=as_of)
    authoritative = next((link for link in links if link.is_authoritative), None)
    assay_level_digest = authoritative.allele.vrs_digest if authoritative is not None else None
    clingen_allele_id = authoritative.allele.clingen_allele_id if authoritative is not None else None

    annotations = get_allele_annotations(db, [link.allele for link in links], as_of=as_of)

    # Spec-pure Cat-VRS built on the fly, plus the MaveDB layer (mode + per-member relations). The
    # relations key the sidecar below; the defining allele is deliberately absent from them, so it
    # gets relation=None.
    transit = categorical_variant_for_variant(db, variant.id, name=variant.urn or "", as_of=as_of)
    if transit is not None:
        molecular_representation = transit.categorical_variant.model_dump(mode="json", exclude_none=True)
        mode: Optional[str] = transit.mode.value
        relations = transit.member_relations
    else:
        molecular_representation = None
        mode = None
        relations = {}

    # The per-allele identity sidecar: one entry per linked allele, keyed by VRS digest (the same
    # record-scoped, all-levels link set that seeds `annotations`, so the two maps share keys). Carries
    # the molecular-identity facts the UI labels annotations by — level, reference-frame HGVS (exactly one
    # of hgvs_g/c/p is populated per allele), ClinGen id — and the member→defining relation.
    # Within-record projection grouping: projection_group -> the digests sharing it. A link's projection
    # sibling is the ≤1 *other* digest in its group (the c↔g pair); the protein apex and any pre-RT link
    # carry a NULL group and so appear in no bucket (projection_of stays None for them).
    group_digests: dict[int, list[str]] = {}
    for link in links:
        if link.projection_group is not None and link.allele.vrs_digest is not None:
            group_digests.setdefault(link.projection_group, []).append(link.allele.vrs_digest)

    alleles: dict[str, AlleleIdentity] = {}
    for link in links:
        allele = link.allele
        if allele.vrs_digest is None:
            continue

        # The projection sibling: the other digest in this link's projection_group, if any. A group has
        # ≤2 members, so there is at most one sibling — this allele's projection partner.
        projection_of: Optional[str] = None
        if link.projection_group is not None:
            projection_of = next(
                (digest for digest in group_digests.get(link.projection_group, []) if digest != allele.vrs_digest),
                None,
            )

        relation = relations.get(allele.vrs_digest)
        alleles[allele.vrs_digest] = AlleleIdentity(
            level=allele.level,
            hgvs=allele.hgvs_g or allele.hgvs_c or allele.hgvs_p,
            clingen_allele_id=allele.clingen_allele_id,
            relation=relation.value if relation is not None else None,
            derivation=_derivation_for(is_authoritative=link.is_authoritative, assay_level=assay_level).value,
            projection_of=projection_of,
        )

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
        alleles=alleles,
        annotations=annotations,
        is_current=superseding_score_set is None,
        superseded_by_score_set=superseding_score_set.urn if superseding_score_set is not None else None,
    )
