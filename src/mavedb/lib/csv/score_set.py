"""The score-set CSV export: every variant in one score set, and the columns it can offer."""

from collections.abc import Iterator
from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.columns import CsvPlan, render_csv
from mavedb.lib.csv.entries import (
    MAPPING_DERIVED_NAMESPACES,
    AvailableCsvNamespaceEntry,
    calibration_namespace_entries,
    calibration_viewer,
    clinvar_namespace_entries,
    clinvar_release_namespaces,
    score_sets_have_current_mappings,
    static_namespace_entry,
)
from mavedb.lib.csv.fetch import CSV_BATCH_SIZE, ScoreSetScope, iter_csv_rows, unused_hgvs_columns
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
    as_of: Optional[datetime] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
) -> str:
    """Get the variant data from a score set as a CSV string.

    The whole of :func:`stream_score_set_variants_as_csv`, for callers that need the file in one piece.
    """
    return "".join(
        stream_score_set_variants_as_csv(
            db,
            score_set,
            namespaces,
            namespaced=namespaced,
            start=start,
            limit=limit,
            drop_unused_hgvs_columns_flag=drop_unused_hgvs_columns_flag,
            as_of=as_of,
            viewer=viewer,
        )
    )


def stream_score_set_variants_as_csv(
    db: Session,
    score_set: ScoreSet,
    namespaces: List[str],
    *,
    namespaced: bool = False,
    start: Optional[int] = None,
    limit: Optional[int] = None,
    drop_unused_hgvs_columns_flag: Optional[bool] = None,
    as_of: Optional[datetime] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
    batch_size: int = CSV_BATCH_SIZE,
) -> Iterator[str]:
    """Get the variant data from a score set as CSV text, one chunk per batch of ``batch_size`` rows.

    The first chunk starts with the header, and an export with no rows is the header alone. Nothing is read
    until the first chunk is requested, and the rows come from a server-side cursor that holds ``db``'s
    transaction open until the last chunk is read or the generator is closed.
    """
    # `dataset_columns` is NOT NULL with a `{}` default, so only a transient score set reaches this unset.
    # Treating it as empty if it is unset yields the core columns alone rather than failing  deeper in
    # column planning.
    plan = CsvPlan.build(score_set.dataset_columns or {}, namespaces, namespaced=namespaced)

    # TODO(#372): non-null id fields
    scope = ScoreSetScope(score_set_id=score_set.id, start=start, limit=limit)  # type: ignore[arg-type]
    if drop_unused_hgvs_columns_flag:
        plan = plan.without(unused_hgvs_columns(db, scope))

    rows = iter_csv_rows(db, plan, scope, as_of=as_of, viewer=viewer, batch_size=batch_size)
    yield from render_csv(plan, rows)


def available_score_set_csv_namespaces(
    db: Session,
    score_set: ScoreSet,
    viewer: Optional[ScoreCalibrationViewer] = None,
    *,
    as_of: Optional[datetime] = None,
) -> list[AvailableCsvNamespaceEntry]:
    """Every namespace the score-set CSV can serve data for, labeled and grouped for a picker.

    Its own endpoint rather than a field on the score-set response: it costs several queries and is only
    needed when a download dialog opens. A namespace absent here is still accepted by the CSV endpoint;
    it just produces a column of NA.

    ``as_of`` must match the instant the caller will download at. Discovery pinned to "now" against an
    ``as_of`` download disagrees in both directions: it offers mapping namespaces that come back entirely
    NA, and omits ones the download could have filled. The ClinVar case is the sharpest, since the live
    release set decides which ``clinvar.YYYY_MM`` headers exist at all.
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

    if score_sets_have_current_mappings(db, score_set_ids, as_of=as_of):
        entries.extend(static_namespace_entry(ns) for ns in MAPPING_DERIVED_NAMESPACES)
        entries.extend(clinvar_namespace_entries(clinvar_release_namespaces(db, score_set_ids, as_of=as_of)))

    # Every calibration the score set defines is offered, rangeless ones included.
    calibrations = db.scalars(
        select(ScoreCalibration)
        .options(
            selectinload(ScoreCalibration.score_set),
            selectinload(ScoreCalibration.functional_classifications),  # read by the eligibility check
            selectinload(ScoreCalibration.superseding_calibration),  # read by the superseded flag
        )
        .where(and_(ScoreCalibration.score_set_id == score_set.id, ScoreCalibration.urn.is_not(None)))
    ).all()
    entries.extend(calibration_namespace_entries(calibration_viewer(viewer).visible(calibrations)))

    # `relationship` is absent by design: match_type describes a row's relation to a requested record,
    # which only the variant CSV has.
    return entries
