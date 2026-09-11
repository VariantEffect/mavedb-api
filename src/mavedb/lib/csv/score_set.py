"""The score-set CSV export: every variant in one score set, and the columns it can offer."""

from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.annotations import annotations_for_rows
from mavedb.lib.csv.columns import (
    assemble_csv_headers,
    drop_unused_hgvs_columns,
    plan_csv_columns,
    rows_to_csv,
    variants_to_csv_rows,
)
from mavedb.lib.csv.entries import (
    AvailableCsvNamespaceEntry,
    calibration_namespace_entries,
    calibration_viewer,
    clinvar_namespace_entries,
    clinvar_release_namespaces,
    score_sets_have_current_mappings,
    static_namespace_entry,
)
from mavedb.lib.csv.fetch import fetch_variant_csv_data
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.mave.constants import REQUIRED_SCORE_COLUMN
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet


def get_score_set_variants_as_csv(
    db: Session,
    score_set: ScoreSet,
    namespaces: List[str],
    namespaced: bool = False,
    start: Optional[int] = None,
    limit: Optional[int] = None,
    drop_unused_hgvs_columns_flag: Optional[bool] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
) -> str:
    """Get the variant data from a score set as a CSV string."""
    # `dataset_columns` is NOT NULL with a `{}` default, so only a transient score set reaches this unset.
    # Treating it as empty if it is unset yields the core columns alone rather than failing  deeper in
    # column planning.
    plan = plan_csv_columns(score_set.dataset_columns or {}, namespaces)

    fetched = fetch_variant_csv_data(
        db,
        plan.namespaced_columns,
        plan.clinvar_namespaces,
        score_set=score_set,
        start=start,
        limit=limit,
    )

    mappings = fetched.mappings or [None] * len(fetched.variants)
    rows_data = variants_to_csv_rows(
        fetched.variants,
        columns=plan.namespaced_columns,
        namespaced=namespaced,
        mappings=fetched.mappings,
        gnomad_data=fetched.gnomad_data,
        clinvar_data_by_ns=fetched.clinvar_per_variant,
        annotations_by_ns=annotations_for_rows(db, fetched.variants, mappings, plan.calibration_namespaces, viewer),
    )

    rows_columns = assemble_csv_headers(plan.namespaced_columns, namespaced=namespaced)

    if drop_unused_hgvs_columns_flag:
        rows_data, rows_columns = drop_unused_hgvs_columns(rows_data, rows_columns)

    return rows_to_csv(rows_data, rows_columns)


def available_score_set_csv_namespaces(
    db: Session,
    score_set: ScoreSet,
    viewer: Optional[ScoreCalibrationViewer] = None,
) -> list[AvailableCsvNamespaceEntry]:
    """Every namespace the score-set CSV can serve data for, labeled and grouped for a picker.

    Its own endpoint rather than a field on the score-set response: it costs several queries and is only
    needed when a download dialog opens. A namespace absent here is still accepted by the CSV endpoint;
    it just produces a column of NA.
    """
    dataset_columns = score_set.dataset_columns if isinstance(score_set.dataset_columns, dict) else {}
    # TODO(#372): non-null id fields
    score_set_ids: list[int] = [score_set.id]  # type: ignore

    score_columns = [str(column) for column in dataset_columns.get("score_columns", [])]

    entries: list[AvailableCsvNamespaceEntry] = []
    if score_columns:
        entries.append(static_namespace_entry(CsvNamespace.SCORES))
    if any(column != REQUIRED_SCORE_COLUMN for column in score_columns):
        entries.append(static_namespace_entry(CsvNamespace.SCORES_CUSTOM))
    if dataset_columns.get("count_columns"):
        entries.append(static_namespace_entry(CsvNamespace.COUNTS))

    entries.append(static_namespace_entry(CsvNamespace.SCORE_SET))  # always its own provenance

    if score_sets_have_current_mappings(db, score_set_ids):
        entries.extend(
            static_namespace_entry(ns)
            for ns in (CsvNamespace.REFERENCE_HGVS, CsvNamespace.VEP, CsvNamespace.GNOMAD, CsvNamespace.CLINGEN)
        )
        entries.extend(clinvar_namespace_entries(clinvar_release_namespaces(db, score_set_ids)))

    # Every calibration the score set defines is offered, rangeless ones included.
    calibrations = db.scalars(
        select(ScoreCalibration)
        .options(
            selectinload(ScoreCalibration.score_set),
            selectinload(ScoreCalibration.functional_classifications),  # read by the eligibility check
        )
        .where(and_(ScoreCalibration.score_set_id == score_set.id, ScoreCalibration.urn.is_not(None)))
    ).all()
    entries.extend(calibration_namespace_entries(calibration_viewer(viewer).visible(calibrations)))

    # `relationship` is absent by design: match_type describes a row's relation to a requested record,
    # which only the variant CSV has.
    return entries
