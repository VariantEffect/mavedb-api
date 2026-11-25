"""The rows a CSV export renders: the variants its scope names, each filled in with the sources its plan reads."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Union

from sqlalchemy import ColumnElement, Select, func, not_, or_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.columns import CsvPlan
from mavedb.lib.csv.sources import CsvContext, source_filler
from mavedb.lib.csv.specs import CsvRow, RowSource
from mavedb.lib.mave.utils import NULL_VALUES
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.validation.constants.general import hgvs_columns
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

# CSV rows come out in variant-number order (the integer after '#'); id breaks ties stably.
_VARIANT_NUMBER_ORDER = (Variant.variant_number, Variant.id)

CSV_BATCH_SIZE = 5000
"""Rows a streamed score set CSV reads, renders and sends at a time. Large enough for efficient fetching, small
enough that a batch's rows and rendered text stay a few megabytes."""

_NULL_TOKENS = sorted({value.lower() for value in NULL_VALUES})
"""``NULL_VALUES`` lowercased for matching in SQL: the tokens ``columns._is_output_null`` writes as NA."""

# Python's str.strip() also strips a few control and Unicode space characters; these are the ones an HGVS
# string could plausibly carry.
_PYTHON_STRIP_WHITESPACE = " \t\n\r\x0b\x0c"
"""The characters ``btrim`` strips to match ``_is_output_null``'s ``str.strip()``."""


@dataclass(frozen=True)
class ScoreSetScope:
    """Every variant of one score set in variant-number order, or one ``start``/``limit`` page of them."""

    score_set_id: int
    start: Optional[int] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class VariantScope:
    """An explicit set of variants, in the order given."""

    variant_ids: Sequence[int]
    match_type: Optional[str] = None
    """How each row relates to the record the caller asked for, reported in the ``relationship`` columns."""


CsvScope = Union[ScoreSetScope, VariantScope]


def iter_csv_rows(
    db: Session,
    plan: CsvPlan,
    scope: CsvScope,
    *,
    as_of: Optional[datetime] = None,
    viewer: Optional[ScoreCalibrationViewer] = None,
    batch_size: int = CSV_BATCH_SIZE,
) -> Iterator[list[CsvRow]]:
    """The rows of *scope*, each carrying every source *plan* reads, in batches of up to *batch_size*.

    A score set's variants come from a server-side cursor, and each batch's sources are loaded for that batch
    alone, so memory doesn't grow with the score set. The query runs when the first batch is requested, and the
    cursor holds ``db``'s transaction open until the last batch is read or the generator is closed. An explicit
    variant set is one batch.

    Args:
        as_of: reconstruct the mapping-derived sources as they stood at this instant, over the variants'
            immutable submitted HGVS, scores and counts. Defaults to currently-live rows.
        viewer: who the calibration interpretations are read for. Omitted, only public calibrations are.
    """
    fill = source_filler(CsvContext(db, as_of=as_of, viewer=viewer), plan)
    match_type = scope.match_type if isinstance(scope, VariantScope) else None

    for variants in _variant_batches(db, plan, scope, batch_size):
        rows: list[CsvRow] = [{RowSource.VARIANT: variant, RowSource.MATCH_TYPE: match_type} for variant in variants]
        fill(rows)
        yield rows


def _variant_batches(db: Session, plan: CsvPlan, scope: CsvScope, batch_size: int) -> Iterator[Sequence[Variant]]:
    query = select(Variant)
    if plan.needs_score_set:
        query = query.options(
            selectinload(Variant.score_set).selectinload(ScoreSet.score_calibrations),
            selectinload(Variant.score_set).selectinload(ScoreSet.target_genes),
        )

    if isinstance(scope, VariantScope):
        variants = db.scalars(query.where(Variant.id.in_(scope.variant_ids))).all()
        # Postgres does not preserve IN-list order.
        position = {variant_id: index for index, variant_id in enumerate(scope.variant_ids)}
        yield sorted(variants, key=lambda variant: position[variant.id])
        return

    if scope.start is None and scope.limit is None:
        query = query.where(Variant.score_set_id == scope.score_set_id)
    else:
        # Joining to the page's ids, with no score set filter of its own, leaves the outer query nothing to do
        # but fetch those rows. Filtered by score set, the planner could serve its ORDER BY by walking the whole
        # score set in index order and reading every row, skipped ones included (#901).
        page = _page_variant_ids(scope.score_set_id, scope.start, scope.limit).subquery("page")
        query = query.join(page, page.c.id == Variant.id)

    query = query.order_by(*_VARIANT_NUMBER_ORDER)
    yield from db.scalars(query, execution_options={"yield_per": batch_size}).partitions()


def _page_variant_ids(score_set_id: int, start: Optional[int], limit: Optional[int]) -> Select[Any]:
    """The ids of one ``start``/``limit`` page of a score set's variants. A ``None`` bound is left off.

    Selecting only ids lets the variant-number index skip the rows before ``start`` without joining them
    (#901).
    """
    query = select(Variant.id).where(Variant.score_set_id == score_set_id).order_by(*_VARIANT_NUMBER_ORDER)
    if start is not None:
        query = query.offset(start)
    if limit is not None:
        query = query.limit(limit)
    return query


def unused_hgvs_columns(db: Session, scope: ScoreSetScope) -> set[str]:
    """The core HGVS columns that are null on every row of *scope*.

    One aggregate query, so an export knows its header before it renders a row. A value counts as null exactly
    when the export would write it as NA (``columns._is_output_null``).
    """
    hgvs = select(*(getattr(Variant, column) for column in hgvs_columns))
    if scope.start is None and scope.limit is None:
        rows = hgvs.where(Variant.score_set_id == scope.score_set_id).subquery()
    else:
        page = _page_variant_ids(scope.score_set_id, scope.start, scope.limit).subquery("page")
        rows = hgvs.join(page, page.c.id == Variant.id).subquery()

    present = db.execute(select(*(func.bool_or(not_(_renders_as_na(rows.c[column]))) for column in hgvs_columns))).one()
    # bool_or over no rows is NULL: an empty export uses none of them.
    return {column for column, used in zip(hgvs_columns, present) if not used}


def _renders_as_na(column: ColumnElement[Any]) -> ColumnElement[bool]:
    """Whether the export writes this text column's value as NA, the SQL twin of ``columns._is_output_null``."""
    # A null renders as the string "None", which is itself a null token.
    return or_(column.is_(None), func.lower(func.btrim(column, _PYTHON_STRIP_WHITESPACE)).in_(_NULL_TOKENS))
