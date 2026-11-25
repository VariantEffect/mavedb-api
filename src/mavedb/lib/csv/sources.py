"""The per-row data a CSV export reads beyond the variant itself, and how each source is loaded.

Adding a source means adding a ``RowSource``, a loader registered here and a namespace spec that reads it.
Nothing that plans, fetches or writes rows changes. A loader reading time-versioned rows must honor
``ctx.as_of``, and ``tests/lib/csv/test_coverage.py`` fails until each loader declares how that is shown.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from mavedb.lib.annotation.flatten import flatten_annotation
from mavedb.lib.csv.annotations import calibrations_for_namespaces, containing_classification_ids
from mavedb.lib.csv.columns import CsvPlan
from mavedb.lib.csv.namespaces import CLINVAR_DB_NAME
from mavedb.lib.csv.specs import CsvMappedRow, CsvRow, RowSource
from mavedb.lib.gnomad import GNOMAD_DB_NAME
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


@dataclass(frozen=True)
class CsvContext:
    """What every loader of one export shares."""

    db: Session
    """The request's session. Loaders must not open their own: the caller's identity is bound to this one."""
    as_of: Optional[datetime] = None
    """Reconstruct the mapping-derived sources as they stood at this instant, rather than as they are now."""
    viewer: Optional[ScoreCalibrationViewer] = None


BatchLoader = Callable[[Sequence[CsvRow]], Sequence[Any]]
"""Loads one source for a batch of rows: one value per row, in row order."""


@dataclass(frozen=True)
class SourceLoader:
    """How one source is loaded for an export's rows."""

    prepare: Callable[[CsvContext, Mapping[str, str]], BatchLoader]
    """Called once per export with the source's requested parameters (namespace -> release or URN, empty for
    an unparameterized source). Does the work every batch shares and returns the per-batch loader."""

    requires: frozenset[RowSource] = frozenset()
    """The sources this loader reads off each row, which are loaded before it."""


SCOPE_SOURCES = frozenset({RowSource.VARIANT, RowSource.MATCH_TYPE})
"""The sources an export's scope supplies with each row, rather than a loader."""


def _variant_ids(rows: Sequence[CsvRow]) -> list[int]:
    return [row[RowSource.VARIANT].id for row in rows]


def _allele_id(row: CsvRow) -> Optional[int]:
    mapping: Optional[CsvMappedRow] = row[RowSource.MAPPING]
    return mapping.allele_id if mapping is not None else None


def _allele_ids(rows: Sequence[CsvRow]) -> list[int]:
    return [allele_id for allele_id in map(_allele_id, rows) if allele_id is not None]


def _load_mappings(ctx: CsvContext, _parameters: Mapping[str, str]) -> BatchLoader:
    def load(rows: Sequence[CsvRow]) -> list[Optional[CsvMappedRow]]:
        by_variant = _mappings_by_variant(ctx.db, _variant_ids(rows), as_of=ctx.as_of)
        return [by_variant.get(row[RowSource.VARIANT].id) for row in rows]

    return load


def _mappings_by_variant(db: Session, variant_ids: list[int], *, as_of: Optional[datetime]) -> dict[int, CsvMappedRow]:
    """Each variant's live mapping record, with its authoritative allele (digest, ClinGen id, VEP consequence)
    and that allele's measured projection group (the mapped HGVS at every level).

    A variant with no live record has no entry. A record with no authoritative allele, which failed and
    benign-absent mappings leave, still has one, with its allele-derived fields empty.
    """
    if not variant_ids:
        return {}

    rows = db.execute(
        select(
            MappingRecord.variant_id,
            MappingRecord.hgvs_assay_level,
            Allele.id.label("allele_id"),
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
            ProjectionGroup.hgvs_c,
            ProjectionGroup.hgvs_g,
            ProjectionGroup.hgvs_p,
        )
        .outerjoin(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                MappingRecordAllele.live_at(as_of),
            ),
        )
        .outerjoin(Allele, Allele.id == MappingRecordAllele.allele_id)
        .outerjoin(
            VepAlleleConsequence,
            and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.live_at(as_of)),
        )
        .outerjoin(
            ProjectionGroup,
            and_(ProjectionGroup.id == MappingRecordAllele.projection_group, ProjectionGroup.live_at(as_of)),
        )
        .where(MappingRecord.variant_id.in_(variant_ids), MappingRecord.live_at(as_of))
    )

    return {
        row.variant_id: CsvMappedRow(
            hgvs_g=row.hgvs_g,
            hgvs_c=row.hgvs_c,
            hgvs_p=row.hgvs_p,
            hgvs_assay_level=row.hgvs_assay_level,
            vrs_digest=row.vrs_digest,
            vep_functional_consequence=row.functional_consequence,
            clingen_allele_id=row.clingen_allele_id,
            allele_id=row.allele_id,
        )
        for row in rows
    }


def _load_gnomad(ctx: CsvContext, _parameters: Mapping[str, str]) -> BatchLoader:
    def load(rows: Sequence[CsvRow]) -> list[Optional[GnomADVariant]]:
        by_allele = _gnomad_by_allele(ctx.db, _allele_ids(rows), as_of=ctx.as_of)
        return [by_allele.get(allele_id) if (allele_id := _allele_id(row)) is not None else None for row in rows]

    return load


def _gnomad_by_allele(db: Session, allele_ids: list[int], *, as_of: Optional[datetime]) -> dict[int, GnomADVariant]:
    """The live gnomAD frequency record for each of *allele_ids*, keyed by allele id.

    Liveness alone selects the release, with no ``db_version`` predicate. The link is single-live per allele
    and ingestion supersedes on that key, so whatever is live is the release we hold for that allele. Alleles
    absent from a newer release keep their older link, so mixed versions are normal; the ``gnomad_version``
    column reports each row's.
    """
    if not allele_ids:
        return {}

    rows = db.execute(
        select(GnomadAlleleLink.allele_id, GnomADVariant)
        .join(GnomADVariant, GnomADVariant.id == GnomadAlleleLink.gnomad_variant_id)
        .where(GnomadAlleleLink.allele_id.in_(allele_ids))
        .where(GnomadAlleleLink.live_at(as_of))
        .where(GnomADVariant.db_name == GNOMAD_DB_NAME)
    )
    return {allele_id: gnomad_variant for allele_id, gnomad_variant in rows.tuples()}


def _load_clinvar(ctx: CsvContext, releases: Mapping[str, str]) -> BatchLoader:
    namespaces_by_version: dict[str, list[str]] = {}
    for namespace, db_version in releases.items():
        namespaces_by_version.setdefault(db_version, []).append(namespace)

    def load(rows: Sequence[CsvRow]) -> list[dict[str, ClinvarControl]]:
        by_allele = _clinvar_by_allele(ctx.db, _allele_ids(rows), namespaces_by_version, as_of=ctx.as_of)
        return [by_allele.get(allele_id, {}) if (allele_id := _allele_id(row)) is not None else {} for row in rows]

    return load


def _clinvar_by_allele(
    db: Session, allele_ids: list[int], namespaces_by_version: dict[str, list[str]], *, as_of: Optional[datetime]
) -> dict[int, dict[str, ClinvarControl]]:
    """For each of *allele_ids*, its live control in each requested release, keyed by the namespaces naming it.

    ClinVar links are multi-live, one per release, so one query reads every requested release (#845).
    """
    if not allele_ids:
        return {}

    rows = db.execute(
        select(ClinvarAlleleLink.allele_id, ClinvarControl)
        .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
        .where(ClinvarAlleleLink.allele_id.in_(allele_ids))
        .where(ClinvarAlleleLink.live_at(as_of))
        .where(ClinvarControl.db_name == CLINVAR_DB_NAME, ClinvarControl.db_version.in_(namespaces_by_version))
    )

    by_allele: dict[int, dict[str, ClinvarControl]] = {}
    for allele_id, control in rows.tuples():
        for namespace in namespaces_by_version[control.db_version]:
            by_allele.setdefault(allele_id, {})[namespace] = control

    return by_allele


def _load_calibration_annotations(ctx: CsvContext, calibration_urns: Mapping[str, str]) -> BatchLoader:
    """Each row's interpretation under every requested calibration.

    A calibration from another score set than the row's leaves its namespace empty, since a score from one
    assay means nothing under another's thresholds. So does one the viewer may not read, and an unmapped row.
    """
    calibrations = calibrations_for_namespaces(ctx.db, dict(calibration_urns), ctx.viewer)

    def load(rows: Sequence[CsvRow]) -> list[dict[str, Any]]:
        membership = containing_classification_ids(ctx.db, _variant_ids(rows))
        annotations = []
        for row in rows:
            variant = row[RowSource.VARIANT]
            contained = membership.get(variant.id, set())
            by_namespace: dict[str, Any] = {}
            for namespace in calibration_urns:
                calibration = calibrations.get(namespace)
                if (
                    row[RowSource.MAPPING] is not None
                    and calibration is not None
                    and calibration.score_set_id == variant.score_set_id
                ):
                    by_namespace[namespace] = flatten_annotation(variant, calibration, contained)
            annotations.append(by_namespace)

        return annotations

    return load


SOURCE_LOADERS: dict[RowSource, SourceLoader] = {
    RowSource.MAPPING: SourceLoader(_load_mappings),
    RowSource.GNOMAD: SourceLoader(_load_gnomad, requires=frozenset({RowSource.MAPPING})),
    RowSource.CLINVAR_ENTRY: SourceLoader(_load_clinvar, requires=frozenset({RowSource.MAPPING})),
    RowSource.ANNOTATION: SourceLoader(_load_calibration_annotations, requires=frozenset({RowSource.MAPPING})),
}
"""Every loaded source's loader, in the order they run: a loader comes after every source it requires."""


def source_filler(ctx: CsvContext, plan: CsvPlan) -> Callable[[Sequence[CsvRow]], None]:
    """Prepare the loaders *plan* needs, and return what fills each batch of rows with their sources."""
    needed = set(plan.sources)
    for source, loader in reversed(SOURCE_LOADERS.items()):
        if source in needed:
            needed |= loader.requires

    loaders = [
        (source, loader.prepare(ctx, plan.parameters.get(source, {})))
        for source, loader in SOURCE_LOADERS.items()
        if source in needed
    ]

    def fill(rows: Sequence[CsvRow]) -> None:
        for source, load in loaders:
            for row, value in zip(rows, load(rows), strict=True):
                row[source] = value

    return fill
