"""The variant page's measurements list — ``GET /clingen-alleles/{caid}/measurements``.

Given a ClinGen allele id — a nucleotide ``CA`` or a protein ``PA`` — return every measurement related to
it across sequence levels. Two measurements are related when their mapping records share an allele (in the
mapping + reverse-translation graph). A ``CA`` anchors on its nt alleles; a ``PA`` on the protein allele.
Each measurement is labelled by its own measured level relative to the query: ``direct`` (measured at the
query), ``protein_consequence``, or ``nucleotide_encoding``.

Example. Reference codon ``TTT`` (Phe200): both ``c.598T>C`` and the sibling ``c.600T>A`` encode
``p.Phe200Leu``. Call them ``CA1``, ``CA2``, ``PA1`` — X assays ``c.598T>C`` (nt), Y assays
``p.Phe200Leu`` (protein), Z assays ``c.600T>A`` (nt):

    query CA1  → X direct, Y protein_consequence, Z nucleotide_encoding
    query PA1  → Y direct, X and Z nucleotide_encoding

A CA query reaches its protein consequence (Y) through the shared protein node ``PA1``. This is what makes
Y reachable when a protein assay has not been reverse-translated: its record then links only the protein
node, with no nt allele for the CA anchor to match, so the apex is the only path to it. The sibling Z is
reached without the apex — reverse translation links every record to its full synonymous nt set, so Z's
record already links ``CA1`` directly. There is no page/search distinction: every surface runs one query.

The protein-consequence step is resolved once, in :func:`_resolve_protein_apex`, which also measures it:
a consequence resolving to more than one protein id (a data-integrity signal) is logged, and the number of
consequences reached with no reverse-translation data of their own is written to the request log.

Authorization (``has_permission``, per ``lib/score_sets.py``): score-set READ gates whether a measurement
appears; calibration READ gates only its inline classification. ``as_of`` reconstructs which records and
links were live at a past instant; scores and calibrations are as-of-invariant.

Distinct from :func:`lib.variant_detail.get_variant_detail` — the record-scoped detail of one measurement.
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, aliased, joinedload

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.score_calibrations import calibration_preference_key, classification_evidence_strength
from mavedb.lib.types.authentication import UserData
from mavedb.lib.variants import variant_score
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table as classification_variants,
)
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)


class MeasurementRelationship(str, Enum):
    """How a measurement relates to the queried ClinGen id, by the measurement's *measured* level."""

    direct = "direct"  # assayed at the queried allele
    protein_consequence = "protein_consequence"  # protein measurement of a CA's consequence
    # an nt measurement of a PA's encoding, or a CA's sibling nt change (encodes the same protein)
    nucleotide_encoding = "nucleotide_encoding"


@dataclass(frozen=True)
class AlleleMeasurement:
    """One measurement in the query's equivalence class (transit; serialized by ``view_models``).

    ``assay_level`` is where this measurement was assayed (always shown, the clinically load-bearing
    fact). ``preferred_classification`` is the readable classification the UI defaults to (primary-first
    cascade, RUO excluded; ``None`` when none applies or the calibration is unreadable).
    """

    variant_urn: str
    score: Optional[float]
    assay_level: Optional[SequenceLevel]
    relationship: str
    assay_level_hgvs: Optional[str]
    submitted_hgvs: Optional[str]
    score_set_urn: str
    score_set_title: str
    preferred_classification: Optional[ScoreCalibrationFunctionalClassification]
    is_current: bool
    superseded_by_score_set: Optional[str]


@dataclass(frozen=True)
class _ProteinApex:
    """Protein consequence(s) co-membered with a CA anchor's nt alleles — the shared node a CA query
    reaches protein measurements through. One PAID is well-formed; ``len(caids) > 1`` is a data-quality
    signal (a duplicate or divergent consequence). ``unregistered`` counts apex rows without a PAID yet.
    """

    allele_ids: list[int]
    caids: list[str]
    unregistered: int


def _preferred_classification(
    db: Session, variant: Variant, *, user_data: Optional[UserData]
) -> Optional[ScoreCalibrationFunctionalClassification]:
    """The variant's preferred *readable* functional classification, or ``None``.

    Mirrors the UI's calibration cascade — ``primary`` then ``investigator_provided``, strongest evidence
    within a tier, then ``id`` for determinism. RUO calibrations are excluded outright (never shown here),
    and calibrations the caller can't read are skipped rather than blanking a readable call. Kept in sync
    with the UI's ``activeCalibrationOptions`` default.
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
            .where(ScoreCalibration.research_use_only.is_(False))
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
    """Sort: current before superseded; direct before related; strongest evidence (pathogenic wins ties)
    first; newest-published first; then URN for a stable tiebreak."""
    magnitude, direction = classification_evidence_strength(measurement.preferred_classification)
    return (
        0 if measurement.is_current else 1,
        0 if measurement.relationship == MeasurementRelationship.direct else 1,
        0 if measurement.preferred_classification is not None else 1,
        -magnitude,
        direction,
        -(published_date.toordinal()) if published_date is not None else 0,
        measurement.variant_urn,
    )


def _resolve_protein_apex(db: Session, anchor_ids: list[int], *, as_of: Optional[datetime]) -> _ProteinApex:
    """The protein consequence(s) co-membered with the nt ``anchor_ids`` via a shared live record.

    The single site the apex is resolved, so its cardinality (see :class:`_ProteinApex`) is measured
    rather than being a silent side effect of the union. Reads ``clingen_allele_id`` to count PAIDs.
    """
    anchor_link = aliased(MappingRecordAllele)
    rows = db.execute(
        select(Allele.id, Allele.clingen_allele_id)
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .join(anchor_link, anchor_link.mapping_record_id == MappingRecordAllele.mapping_record_id)
        .where(anchor_link.allele_id.in_(anchor_ids))
        .where(anchor_link.live_at(as_of))
        .where(MappingRecordAllele.live_at(as_of))
        .where(Allele.level == SequenceLevel.protein.value)
        .distinct()
    ).all()

    return _ProteinApex(
        allele_ids=[row.id for row in rows],
        caids=sorted({row.clingen_allele_id for row in rows if row.clingen_allele_id is not None}),
        unregistered=sum(1 for row in rows if row.clingen_allele_id is None),
    )


def get_allele_measurements(
    db: Session,
    clingen_allele_id: str,
    *,
    user_data: Optional[UserData],
    include_superseded: bool = False,
    as_of: Optional[datetime] = None,
) -> list[AlleleMeasurement]:
    """The measurements in ``clingen_allele_id``'s equivalence class, or ``[]`` if it resolves to no live
    record.

    A CA query returns ``direct`` measurements, its ``protein_consequence`` (reached through the protein
    apex, see the module docstring), and the ``nucleotide_encoding`` siblings. A PA query returns its
    ``direct`` protein measurements and their ``nucleotide_encoding`` encodings.
    """
    anchor = db.execute(select(Allele.id, Allele.level).where(Allele.clingen_allele_id == clingen_allele_id)).all()
    if not anchor:
        return []

    anchor_ids = [row.id for row in anchor]
    entry_is_protein = any(row.level == SequenceLevel.protein.value for row in anchor)

    # A CA query always includes its protein consequence, reached through the shared protein node, so it is
    # reachable even when a protein assay isn't reverse-translated yet (its record then links only the
    # protein node). Transitive, so it's measured: cardinality >1 logged, provenance tallied below. No-op
    # for a PA (already anchored on the protein).
    apex: Optional[_ProteinApex] = None
    if not entry_is_protein:
        apex = _resolve_protein_apex(db, anchor_ids, as_of=as_of)
        anchor_ids += apex.allele_ids
        if len(apex.caids) > 1:
            logger.warning(
                msg=(
                    f"ClinGen allele {clingen_allele_id} resolved to {len(apex.caids)} distinct protein "
                    f"apexes ({', '.join(apex.caids)}); measurements may conflate consequences."
                ),
                extra=logging_context(),
            )

    record_ids = db.scalars(
        select(MappingRecordAllele.mapping_record_id)
        .where(MappingRecordAllele.allele_id.in_(anchor_ids))
        .where(MappingRecordAllele.live_at(as_of))
        .distinct()
    ).all()
    if not record_ids:
        return []

    # Records that carry a derived (RT) link of their own. A protein consequence whose record has none was
    # reached only through the apex of an already resolved protein consequence.
    records_with_derived_links: set[int] = (
        set(
            db.scalars(
                select(MappingRecordAllele.mapping_record_id)
                .where(MappingRecordAllele.mapping_record_id.in_(record_ids))
                .where(MappingRecordAllele.is_authoritative.is_(False))
                .where(MappingRecordAllele.live_at(as_of))
                .distinct()
            ).all()
        )
        if apex is not None
        else set()
    )

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
    protein_consequence_count = 0
    apex_only_unresolved_count = 0
    for record, variant, measured_allele in rows:
        score_set = variant.score_set
        if not has_permission(user_data, score_set, Action.READ).permitted:
            continue

        # Don't leak an unreadable superseding score set.
        superseding = score_set.superseding_score_set
        if superseding is not None and not has_permission(user_data, superseding, Action.READ).permitted:
            superseding = None

        is_current = superseding is None
        if not is_current and not include_superseded:
            continue

        # Label by the measured (authoritative) allele's level relative to the query.
        if measured_allele.clingen_allele_id == clingen_allele_id:
            relationship = MeasurementRelationship.direct
        elif measured_allele.level == SequenceLevel.protein.value:
            relationship = MeasurementRelationship.protein_consequence
        else:
            relationship = MeasurementRelationship.nucleotide_encoding

        # Apex-only provenance: a protein consequence whose record has no derived link was reached purely
        # through the apex.
        if apex is not None and relationship == MeasurementRelationship.protein_consequence:
            protein_consequence_count += 1
            if record.id not in records_with_derived_links:
                apex_only_unresolved_count += 1

        assay_level = SequenceLevel(record.assay_level) if record.assay_level else None
        measurement = AlleleMeasurement(
            variant_urn=variant.urn or "",
            score=variant_score(variant),
            assay_level=assay_level,
            relationship=relationship,
            assay_level_hgvs=record.hgvs_assay_level,
            submitted_hgvs=variant.hgvs_pro if assay_level == SequenceLevel.protein.value else variant.hgvs_nt,
            score_set_urn=score_set.urn or "",
            score_set_title=score_set.title or "",
            preferred_classification=_preferred_classification(db, variant, user_data=user_data),
            is_current=is_current,
            superseded_by_score_set=superseding.urn if superseding is not None else None,
        )
        measurements.append((measurement, score_set.published_date))

    # Publish the apex provenance to the request log so the ambiguous-apex and apex-only-unresolved rates are observable.
    if apex is not None:
        save_to_logging_context(
            {
                "apex_paid_count": len(apex.caids),
                "apex_unregistered_count": apex.unregistered,
                "measurements_protein_consequence": protein_consequence_count,
                "measurements_apex_only_unresolved": apex_only_unresolved_count,
            }
        )

    measurements.sort(key=lambda pair: _ordering_key(pair[0], pair[1]))
    return [measurement for measurement, _ in measurements]
