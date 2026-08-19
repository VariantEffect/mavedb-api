"""Clinically-oriented, variant-level CSV export.

Serves the same interpretation as the variant-level VA-Spec JSON download, but flat: a manuscript
reviewer found the ACMG evidence codes clinically inaccessible when buried in nested evidence lines.

Column layout, fetching, NA handling, and serialization come from the shared engine in this package.
Only the variant-scoped parts live here: finding measurements that share a ClinGen allele, and choosing
default calibration and ClinVar namespaces.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.annotations import annotations_for_rows
from mavedb.lib.csv.columns import (
    assemble_csv_headers,
    plan_csv_columns,
    rows_to_csv,
    variants_to_csv_rows,
)
from mavedb.lib.csv.entries import (
    AvailableCsvNamespaceEntry,
    calibration_can_annotate,
    calibration_namespace_entries,
    clinvar_namespace_entries,
    clinvar_release_namespaces,
    score_sets_have_current_mappings,
    static_namespace_entry,
    calibration_viewer,
)
from mavedb.lib.csv.fetch import fetch_variant_csv_data
from mavedb.lib.csv.namespaces import (
    CsvNamespace,
    calibration_namespace_for_urn,
    clinvar_namespace_sort_key,
)
from mavedb.lib.mave.utils import NA_VALUE
from mavedb.lib.urns import score_set_urn_sort_key, variant_urn_sort_key
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)


ALWAYS_AVAILABLE_NAMESPACES: list[str] = [
    CsvNamespace.SCORES,
    CsvNamespace.SCORE_SET,
    CsvNamespace.RELATIONSHIP,
]
"""Namespaces every measurement can fill: its score, its score set, and its relation to the request."""

MAPPING_DERIVED_NAMESPACES: list[str] = [
    CsvNamespace.REFERENCE_HGVS,
    CsvNamespace.VEP,
    CsvNamespace.GNOMAD,
    CsvNamespace.CLINGEN,
]
"""Namespaces read from a mapped variant, offered whenever the score set has any mapping.

Scoped to the score set, not the measurement: omitting a column would say "never looked" where the truth
is "looked, found nothing".
"""

BASE_VARIANT_CSV_NAMESPACES: list[str] = ALWAYS_AVAILABLE_NAMESPACES + MAPPING_DERIVED_NAMESPACES
"""The fixed namespaces a mapped variant's CSV includes.

``scores_custom`` and ``counts`` are excluded: they vary across score sets, and this export puts one row
per score set, so their columns would be mostly NA.
"""

EXACT_MATCH_TYPE = "exact"
"""Measurements sharing the requested variant's ClinGen allele ID — currently the only relationship emitted.

TODO(#784): widen ``_equivalent_measurements`` to nucleotide/amino-acid equivalence once #791 lands
``equivalent_nt``/``equivalent_aa``; ``relationship.match_type`` then takes more than this one value.
See https://github.com/VariantEffect/mavedb-api/issues/784
"""


def _equivalent_measurements(
    db: Session,
    variant_urn: str,
    may_read_score_set: Optional[Callable[[ScoreSet], bool]] = None,
    *,
    as_of: Optional[datetime] = None,
) -> Optional[list[tuple[int, int]]]:
    """Resolve a variant URN to the measurements the CSV should report.

    Returns ``[(variant_id, score_set_id), ...]``, requested variant first, then every other measurement
    live as of *as_of* against the same ClinGen allele, ordered by score set and variant URN so repeated
    downloads are byte-identical. A live mapping record and its authoritative allele link are unique by
    construction, so at most one entry exists per variant without needing to pin or deduplicate.

    Args:
        may_read_score_set: when given, drops measurements from score sets the caller may not read.
            Applied only to score sets reached by the widening; the caller's own permission check on the
            requested variant is not repeated here.

    Returns:
        None when the variant has no live mapping as of *as_of*, since there is then no allele to expand by.
    """
    requested = db.execute(
        select(Variant.id, Variant.score_set_id, Allele.clingen_allele_id)
        .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of)))
        .join(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                MappingRecordAllele.live_at(as_of),
            ),
        )
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .where(Variant.urn == variant_urn)
        .limit(1)
    ).one_or_none()

    if requested is None:
        return None

    requested_variant_id, requested_score_set_id, clingen_allele_id = requested

    # TODO(#372): non-null id fields
    if not clingen_allele_id:
        return [(requested_variant_id, requested_score_set_id)]  # type: ignore

    equivalents = db.execute(
        select(Variant.id, ScoreSet.id, ScoreSet.urn, Variant.urn)
        .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of)))
        .join(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                MappingRecordAllele.live_at(as_of),
            ),
        )
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .join(Variant.score_set)
        .where(
            and_(
                Allele.clingen_allele_id == clingen_allele_id,
                # By variant, not by mapping: the anchor already represents this variant, and its own
                # live mapping is the same measurement again, not an equivalent one.
                Variant.id != requested_variant_id,
            )
        )
    ).all()

    if may_read_score_set is not None and equivalents:
        candidate_score_set_ids = {row[1] for row in equivalents}
        readable_score_set_ids = {
            score_set.id
            for score_set in db.scalars(select(ScoreSet).where(ScoreSet.id.in_(candidate_score_set_ids))).all()
            if may_read_score_set(score_set)
        }
        equivalents = [row for row in equivalents if row[1] in readable_score_set_ids]

    equivalents = sorted(
        equivalents,
        key=lambda row: (score_set_urn_sort_key(row[2]), variant_urn_sort_key(row[3])),
    )

    return [(requested_variant_id, requested_score_set_id)] + [(row[0], row[1]) for row in equivalents]


def _unmapped_variant_namespaces(db: Session, score_set_id: int, *, as_of: Optional[datetime] = None) -> list[str]:
    """The namespaces to offer for a variant that exists but has no mapping live as of *as_of*.

    Always score, score set, and relationship; plus the mapping-derived groups when the score set has been
    mapped at all, where NA is the honest value for a variant the mapper has not reached.
    """
    if score_sets_have_current_mappings(db, [score_set_id], as_of=as_of):
        return list(BASE_VARIANT_CSV_NAMESPACES)
    return list(ALWAYS_AVAILABLE_NAMESPACES)


def _latest_clinvar_namespace(
    db: Session, score_set_ids: list[int], *, as_of: Optional[datetime] = None
) -> Optional[str]:
    """The ClinVar namespace for the most recent release covering these measurements' score sets.

    One release for the whole file rather than each variant's own latest: that keeps the release in the
    column header where it is citable, and avoids mixing calls from different releases in one column.
    """
    namespaces = clinvar_release_namespaces(db, score_set_ids, as_of=as_of)
    if not namespaces:
        return None

    return max(namespaces, key=clinvar_namespace_sort_key)


def _annotatable_calibration_namespaces(
    db: Session,
    score_set_ids: list[int],
    viewer: Optional[ScoreCalibrationViewer] = None,
) -> dict[str, ScoreCalibration]:
    """Map calibration namespace to calibration, for every calibration eligible to annotate these variants.

    A measurement is only interpretable under its own score set's calibrations, so widening across score
    sets widens this set too; a row shows NA under any calibration that does not apply to it.
    Research-use-only calibrations are included here but excluded from the default selection.
    """
    # Keyed on score sets rather than joined through their variants: `ScoreSet.variants` multiplies the
    # join by every variant before DISTINCT collapses it again, for the same result.
    calibrations = db.scalars(
        select(ScoreCalibration)
        .where(ScoreCalibration.score_set_id.in_(score_set_ids))
        .options(
            selectinload(ScoreCalibration.functional_classifications).selectinload(
                ScoreCalibrationFunctionalClassification.acmg_classification
            ),
            # Each entry reports the score set it belongs to, so a picker can tell one score set's
            # calibrations from another's when the export widens across several.
            selectinload(ScoreCalibration.score_set),
        )
    ).all()

    namespaces: dict[str, ScoreCalibration] = {}
    for calibration in calibration_viewer(viewer).visible(calibrations):
        if not calibration.urn:
            continue

        # Dropped outright, where the score-set export merely leaves it unchecked: a variant's
        # calibrations are scoped to what interprets *this* allele.
        if not calibration_can_annotate(calibration):
            continue

        namespaces[calibration_namespace_for_urn(str(calibration.urn))] = calibration

    return namespaces


def available_variant_csv_namespaces(
    db: Session,
    variant_urn: str,
    may_read_score_set: Optional[Callable[[ScoreSet], bool]] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
    *,
    as_of: Optional[datetime] = None,
) -> list[AvailableCsvNamespaceEntry]:
    """Every namespace the variant CSV can serve data for, labeled and grouped for a picker.

    The fixed namespaces, one ``calibration.<urn>`` per eligible calibration across the variant's
    equivalent measurements, and one ``clinvar.YYYY_MM`` per release covering them.

    Raises:
        ValueError: if no variant with *variant_urn* exists.
    """
    measurements = _equivalent_measurements(db, variant_urn, may_read_score_set=may_read_score_set, as_of=as_of)

    if measurements is None:
        variant = db.scalars(select(Variant).where(Variant.urn == variant_urn).limit(1)).first()
        if variant is None:
            raise ValueError(f"variant with URN '{variant_urn}' not found")

        # No mapping on this variant, but its score set may still be mapped, in which case the
        # mapping-derived columns are owed with NA rather than omitted.
        # TODO(#372): non-null id fields
        return [
            static_namespace_entry(ns)
            for ns in _unmapped_variant_namespaces(db, int(variant.score_set_id), as_of=as_of)  # type: ignore
        ]

    base_entries = [static_namespace_entry(ns) for ns in BASE_VARIANT_CSV_NAMESPACES]

    score_set_ids = list({score_set_id for _, score_set_id in measurements})

    return (
        base_entries
        + calibration_namespace_entries(_annotatable_calibration_namespaces(db, score_set_ids, viewer).values())
        + clinvar_namespace_entries(clinvar_release_namespaces(db, score_set_ids, as_of=as_of))
    )


def get_variant_csv(
    db: Session,
    variant_urn: str,
    namespaces: Optional[list[str]] = None,
    may_read_score_set: Optional[Callable[[ScoreSet], bool]] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
    na_rep: str = NA_VALUE,
    *,
    as_of: Optional[datetime] = None,
) -> str:
    """Build the clinical CSV for a variant and its equivalent measurements.

    One row per measurement: the requested variant first, then every other measurement live as of
    *as_of* against the same ClinGen allele, across score sets.

    Args:
        namespaces: columns to include, in the same vocabulary as the score-set CSV. When omitted,
            defaults to the fixed namespaces plus every eligible calibration and the latest ClinVar
            release covering these measurements.
        as_of: reconstruct every measurement (and its mapping-derived columns) as it stood at this
            instant. Defaults to currently-live rows.

    Raises:
        ValueError: if no variant with *variant_urn* exists.
    """
    measurements = _equivalent_measurements(db, variant_urn, may_read_score_set=may_read_score_set, as_of=as_of)

    if measurements is None:
        return _unmapped_variant_csv(db, variant_urn, namespaces=namespaces, na_rep=na_rep, as_of=as_of)

    variant_ids = [variant_id for variant_id, _ in measurements]
    score_set_ids = list({score_set_id for _, score_set_id in measurements})

    calibrations_by_ns = _annotatable_calibration_namespaces(db, score_set_ids, viewer)

    if namespaces is None:
        clinvar_namespace = _latest_clinvar_namespace(db, score_set_ids, as_of=as_of)
        # Research-use-only calibrations are offerable but never defaulted to. This export is framed clinically.
        default_calibrations = sorted(
            namespace for namespace, calibration in calibrations_by_ns.items() if not calibration.research_use_only
        )
        resolved_namespaces = (
            BASE_VARIANT_CSV_NAMESPACES + default_calibrations + ([clinvar_namespace] if clinvar_namespace else [])
        )
    else:
        resolved_namespaces = list(namespaces)

    # Only the required score column is taken, so the score set's own dataset columns are irrelevant.
    plan = plan_csv_columns(dataset_columns={}, namespaces=resolved_namespaces)
    columns = plan.namespaced_columns

    fetched = fetch_variant_csv_data(
        db,
        columns,
        plan.clinvar_namespaces,
        variant_ids=variant_ids,
        as_of=as_of,
    )

    mappings = fetched.mappings or [None] * len(fetched.variants)

    rows = variants_to_csv_rows(
        fetched.variants,
        columns,
        mappings=fetched.mappings,
        gnomad_data=fetched.gnomad_data,
        clinvar_data_by_ns=fetched.clinvar_per_variant,
        annotations_by_ns=annotations_for_rows(db, fetched.variants, mappings, plan.calibration_namespaces, viewer),
        match_types=[EXACT_MATCH_TYPE] * len(fetched.variants),
        na_rep=na_rep,
        namespaced=True,
    )

    return rows_to_csv(rows, assemble_csv_headers(columns, namespaced=True))


def _unmapped_variant_csv(
    db: Session,
    variant_urn: str,
    namespaces: Optional[list[str]] = None,
    na_rep: str = NA_VALUE,
    *,
    as_of: Optional[datetime] = None,
) -> str:
    """Build the single-row CSV for a variant that exists but has no mapping live as of *as_of*.

    Without a mapping there are no coordinates, no external annotations, no allele to find equivalents by,
    and nothing for the annotation layer to flatten. Identity, score, and provenance still resolve, so the
    download succeeds rather than 404ing on a variant that genuinely exists.
    """
    variant = db.scalars(
        select(Variant)
        .where(Variant.urn == variant_urn)
        .options(
            selectinload(Variant.score_set).selectinload(ScoreSet.target_genes),
        )
    ).one_or_none()

    if variant is None:
        raise ValueError(f"variant with URN '{variant_urn}' not found")

    plan = plan_csv_columns(
        dataset_columns={},
        namespaces=(
            # TODO(#372): non-null id fields
            list(namespaces)
            if namespaces is not None
            else _unmapped_variant_namespaces(db, int(variant.score_set_id), as_of=as_of)  # type: ignore
        ),
    )
    columns = plan.namespaced_columns

    rows: list[dict[str, Any]] = list(
        variants_to_csv_rows(
            [variant],
            columns,
            match_types=[EXACT_MATCH_TYPE],
            na_rep=na_rep,
            namespaced=True,
        )
    )
    return rows_to_csv(rows, assemble_csv_headers(columns, namespaced=True))
