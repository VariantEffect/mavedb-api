"""The ClinGen-allele-centric variant page's measurements list — ``GET /clingen-alleles/{caid}/measurements``.

Given a ClinGen allele id (a nucleotide ``CA`` or a protein ``PA``), list every measurement whose
**cross-layer equivalence class** touches that allele — not just the exact allele. The equivalence
relation is co-membership in a ``MappingRecord``'s allele set (the mapping + reverse-translation graph,
:mod:`lib.alleles`): a ``CA`` anchors on its nucleotide alleles (genomic + coding share the CA), so the
list gathers the nt measurements of that change (**direct**) *and* the protein measurements of its
consequence (**related**); a ``PA`` anchors on the protein allele, gathering the protein measurements of
that change (**direct**) *and* every nt measurement that encodes it (**related**). The asymmetry falls
out of *which* allele anchors the co-membership — a sibling nt change that also encodes the same protein
is not pulled onto a CA page, because its record links the protein but not the anchor nt allele.
``include_nucleotide_siblings`` opts out of that asymmetry for discovery surfaces — see
:func:`get_allele_measurements`.

Distinct from :func:`lib.variant_detail.get_variant_detail`, which is the *record-scoped*, all-levels
detail of one *selected* measurement. This list is the *cross-record* union — who else measured
something in the equivalence class.

Authorization uses ``has_permission`` directly (matching ``lib/score_sets.py``): ``user_data`` is checked
per entity — score-set READ gates whether a measurement appears at all (a private score set's measurement
never leaks), and calibration READ gates the inline classification (withheld while the measurement still
shows). The two-gate split is deliberate: a readable measurement can carry an unreadable classification.
``as_of`` reconstructs the molecular layer (which records/links are live) at a past instant; scores are
immutable and calibrations carry no valid-time, so both are as-of-invariant.
"""

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, aliased, joinedload

from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.score_calibrations import calibration_preference_key, classification_evidence_strength
from mavedb.lib.types.authentication import UserData
from mavedb.lib.variants import variant_score
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table as classification_variants,
)
from mavedb.models.variant import Variant


class MeasurementRelationship(str, Enum):
    """The relationship of a measurement to the queried ClinGen allele. ``direct`` = the measurement was
    assayed *at* this allele; the other two are the RT-related measurements, named for how they relate
    to the query.
    """

    # the measurement was assayed *at* this allele
    direct = "direct"
    # CA query, protein measurement of consequence(N)
    protein_consequence = "protein_consequence"
    # A nt measurement encoding the queried change's protein consequence: a PA query's encodings of P, or —
    # only under ``include_nucleotide_siblings`` — a CA query's sibling nt changes that share consequence(N).
    nucleotide_encoding = "nucleotide_encoding"


@dataclass(frozen=True)
class AlleleMeasurement:
    """One measurement in the queried allele's equivalence class (transit; serialized by
    ``view_models.allele_measurement``).

    ``assay_level`` is the level at which *this* measurement was assayed (protein/cdna/genomic) — the
    clinically load-bearing fact, always shown. ``relationship`` says how it relates to the queried
    ClinGen id. ``primary_classification`` is the primary readable functional classification (``None``
    when the score set has none or the caller cannot read the calibration).
    """

    variant_urn: str
    score: Optional[float]
    assay_level: Optional[str]
    relationship: str
    assay_level_hgvs: Optional[str]
    submitted_hgvs: Optional[str]
    score_set_urn: str
    score_set_title: str
    primary_classification: Optional[ScoreCalibrationFunctionalClassification]
    is_current: bool
    superseded_by_score_set: Optional[str]


def _preferred_classification(
    db: Session, variant: Variant, *, user_data: Optional[UserData]
) -> Optional[ScoreCalibrationFunctionalClassification]:
    """The variant's preferred *readable* functional classification, or ``None``.

    A variant falls into one classification per calibration (non-overlapping ranges); we surface the one
    the UI would default to, mirroring its calibration preference cascade: ``primary`` first, then
    ``investigator_provided``, then non-``research_use_only``. Within a tier we take the strongest
    evidence (the VA-Spec posture — the strongest calibration is the evidence calibration), then ``id``
    only for determinism. The UI's "any with ranges / any calibration" tail steps collapse here: this
    query only surfaces calibrations that already classify the variant. Only calibrations the caller can
    read (calibration READ) are considered, so a private calibration is skipped rather than blanking a
    readable call. Kept in sync with the UI's `activeCalibrationOptions` default selection.
    """
    candidates = [
        (classification, calibration)
        for classification, calibration in db.execute(
            select(ScoreCalibrationFunctionalClassification, ScoreCalibration)
            .join(
                classification_variants,
                classification_variants.c.functional_classification_id == ScoreCalibrationFunctionalClassification.id,
            )
            .join(ScoreCalibration, ScoreCalibration.id == ScoreCalibrationFunctionalClassification.calibration_id)
            .where(classification_variants.c.variant_id == variant.id)
        ).all()
        if has_permission(user_data, calibration, Action.READ).permitted
    ]
    if not candidates:
        return None

    def preference(pair: tuple[ScoreCalibrationFunctionalClassification, ScoreCalibration]) -> tuple:
        classification, calibration = pair
        magnitude, _ = classification_evidence_strength(classification)
        return (
            *calibration_preference_key(calibration),
            -magnitude,
            calibration.id,
        )

    return min(candidates, key=preference)[0]


def _ordering_key(measurement: AlleleMeasurement, published_date: Optional[date]) -> tuple:
    """Sort order:
    1. Direct measurements, then related (protein_consequence / nucleotide_encoding)
    2. Within each, the strongest evidence first (pathogenic wins ties)
    3. Within each, the newest-published first
    4. Within each, the URN for a stable tiebreak
    """
    magnitude, direction = classification_evidence_strength(measurement.primary_classification)
    return (
        0 if measurement.relationship == MeasurementRelationship.direct else 1,
        0 if measurement.primary_classification is not None else 1,
        -magnitude,
        direction,
        -(published_date.toordinal()) if published_date is not None else 0,
        measurement.variant_urn,
    )


def get_allele_measurements(
    db: Session,
    clingen_allele_id: str,
    *,
    user_data: Optional[UserData],
    include_superseded: bool = False,
    include_nucleotide_siblings: bool = False,
    as_of: Optional[datetime] = None,
) -> list[AlleleMeasurement]:
    """List the measurements in ``clingen_allele_id``'s cross-layer equivalence class. Returns
    ``[]`` when the id resolves to no allele or no live record.

    ``include_nucleotide_siblings`` (a ``CA`` entry only; a no-op for ``PA``) widens the class through the
    queried change's protein consequence to also surface the *sibling* nt changes — other nucleotide
    variants that encode the same amino-acid change and were themselves assayed at the nucleotide level
    (``relationship=nucleotide_encoding``).
    """
    anchor = db.execute(select(Allele.id, Allele.level).where(Allele.clingen_allele_id == clingen_allele_id)).all()
    if not anchor:
        return []

    anchor_ids = [row.id for row in anchor]
    entry_is_protein = any(row.level == AnnotationLayer.protein.value for row in anchor)

    # Sibling nucleotide changes (search discovery, CA only): fold the queried change's protein consequence
    # — the protein alleles co-membered with the anchor nt alleles — into the anchor, so the single record
    # union below also reaches every record encoding that consequence. a PA query already anchors on the protein, so
    # this is exactly what makes a CA+siblings query behave like one. (This query would be redundant for a PA entry).
    if include_nucleotide_siblings and not entry_is_protein:
        anchor_link = aliased(MappingRecordAllele)
        anchor_ids += db.scalars(
            select(MappingRecordAllele.allele_id)
            .join(anchor_link, anchor_link.mapping_record_id == MappingRecordAllele.mapping_record_id)
            .join(Allele, Allele.id == MappingRecordAllele.allele_id)
            .where(anchor_link.allele_id.in_(anchor_ids))
            .where(anchor_link.live_at(as_of))
            .where(MappingRecordAllele.live_at(as_of))
            .where(Allele.level == AnnotationLayer.protein.value)
            .distinct()
        ).all()

    record_ids = db.scalars(
        select(MappingRecordAllele.mapping_record_id)
        .where(MappingRecordAllele.allele_id.in_(anchor_ids))
        .where(MappingRecordAllele.live_at(as_of))
        .distinct()
    ).all()
    if not record_ids:
        return []

    # Each record's authoritative (measured) allele fixes the assayed level and the direct/related call.
    authoritative = aliased(MappingRecordAllele)
    rows = (
        db.execute(
            select(MappingRecord, Variant, Allele)
            .join(Variant, Variant.id == MappingRecord.variant_id)
            .join(
                authoritative,
                and_(
                    authoritative.mapping_record_id == MappingRecord.id,
                    authoritative.is_authoritative.is_(True),
                    authoritative.live_at(as_of),
                ),
            )
            .join(Allele, Allele.id == authoritative.allele_id)
            .where(MappingRecord.id.in_(record_ids))
            .where(MappingRecord.live_at(as_of))
            .options(joinedload(Variant.score_set))
        )
        .tuples()
        .all()
    )

    measurements: list[tuple[AlleleMeasurement, Optional[date]]] = []
    for record, variant, measured_allele in rows:
        score_set = variant.score_set
        if not has_permission(user_data, score_set, Action.READ).permitted:
            continue

        # Don't leak unpermitted resources.
        superseding = score_set.superseding_score_set
        if superseding is not None and not has_permission(user_data, superseding, Action.READ).permitted:
            superseding = None

        is_current = superseding is None
        if not is_current and not include_superseded:
            continue

        # Label by the measured level, not the entry level: direct when the measured allele *is* the query,
        # else protein→protein_consequence / nucleotide→nucleotide_encoding. Without siblings the sibling-nt
        # branch is unreachable (a record links the anchor only via itself or its protein consequence).
        if measured_allele.clingen_allele_id == clingen_allele_id:
            relationship = MeasurementRelationship.direct
        elif measured_allele.level == AnnotationLayer.protein.value:
            relationship = MeasurementRelationship.protein_consequence
        else:
            relationship = MeasurementRelationship.nucleotide_encoding

        assay_level = record.assay_level
        measurement = AlleleMeasurement(
            variant_urn=variant.urn or "",
            score=variant_score(variant),
            assay_level=assay_level,
            relationship=relationship,
            assay_level_hgvs=record.hgvs_assay_level,
            submitted_hgvs=variant.hgvs_pro if assay_level == AnnotationLayer.protein.value else variant.hgvs_nt,
            score_set_urn=score_set.urn or "",
            score_set_title=score_set.title or "",
            primary_classification=_preferred_classification(db, variant, user_data=user_data),
            is_current=is_current,
            superseded_by_score_set=superseding.urn if superseding is not None else None,
        )
        measurements.append((measurement, score_set.published_date))

    measurements.sort(key=lambda pair: _ordering_key(pair[0], pair[1]))
    return [measurement for measurement, _ in measurements]
