"""
Script that sweeps the VA-Spec annotation surfaces across the published corpus and reports what broke.

Usage:
```
python3 -m mavedb.scripts.export_sweep --output sweep.csv
```

The annotation test suite runs against constructed variant shapes. Real data contains shapes nobody
thought to construct, and both VA-Spec serialization defects that reached production were of that kind.
This walks the actual corpus and attempts every annotation surface, so the shapes that only exist in the
database get a chance to fail somewhere other than a user's download.

Writes one CSV row per attempted (score set, surface) pair, successes included: a report showing only
failures cannot distinguish "nothing broke" from "nothing ran". Exits non-zero if any pair failed.

Every current mapped variant of every published score set is attempted. The measured cost of sweeping all
score set level surfaces at the current database size is well under an hour for CSV surfaces and a little
over an hour for VA annotation surfaces.

The sweep is read-only and runs as an anonymous principal, matching what a public consumer receives.
Private score calibrations are therefore not exercised; a surface reachable only through a privileged
viewer will report as not-applicable here rather than being annotated.
"""

import csv
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from io import StringIO
from typing import Any, Callable, Optional

import asyncclick as click
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.conformance import AnnotationRoundTripError, round_trip_annotation
from mavedb.lib.annotation.exceptions import EXPECTED_ABSENCE_EXCEPTIONS
from mavedb.lib.csv.score_set import available_score_set_csv_namespaces, get_score_set_variants_as_csv
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_sets import get_current_mapped_variants_for_annotation
from mavedb.lib.urns import variant_urn_sort_key
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)

OK = "ok"
EXCEPTION = "exception"
SCHEMA_VIOLATION = "schema_violation"
SKIPPED = "skipped"

#: Outcomes that mean a surface is broken, as opposed to merely inapplicable. These set the exit code.
FAILURE_OUTCOMES = (EXCEPTION, SCHEMA_VIOLATION)

CSV_COLUMNS = [
    "score_set_urn",
    "surface",
    "outcome",
    "variants_attempted",
    "variants_annotated",
    "variants_not_applicable",
    "variants_failed",
    "first_failing_variant_urn",
    "exception_class",
    "message",
]


def _surfaces(principal: Principal) -> list[tuple[str, Callable[[MappedVariant], Optional[Any]]]]:
    """The three annotation surfaces the API streams, named as their endpoint path segments."""
    return [
        ("study-result", variant_study_result),
        ("functional-statement", partial(variant_functional_impact_statement, principal=principal)),
        ("pathogenicity-statement", partial(variant_pathogenicity_statement, principal=principal)),
    ]


@dataclass
class SurfaceResult:
    """What one annotation surface did across every current mapped variant of one score set."""

    score_set_urn: str
    surface: str
    variants_attempted: int = 0
    variants_annotated: int = 0
    variants_not_applicable: int = 0
    variants_failed: int = 0
    outcome: str = OK
    first_failing_variant_urn: str = ""
    exception_class: str = ""
    message: str = ""
    #: Set when the pair was never attempted, e.g. the score set has no current mapped variants.
    skip_reason: str = ""

    def record_failure(self, variant_urn: str, outcome: str, err: BaseException) -> None:
        """Count a failure, keeping the first one's detail.

        The first is kept rather than the last because a surface that fails on one shape usually fails on
        every variant of that shape, and a hundred identical messages are less useful than one plus a count.
        """
        self.variants_failed += 1
        if self.outcome in FAILURE_OUTCOMES:
            return
        self.outcome = outcome
        self.first_failing_variant_urn = variant_urn or ""
        self.exception_class = type(err).__name__
        # Newlines would break the row apart for anyone reading the CSV with line-oriented tools.
        self.message = " ".join(str(err).split())

    def as_row(self) -> dict[str, Any]:
        return {
            "score_set_urn": self.score_set_urn,
            "surface": self.surface,
            "outcome": self.outcome,
            "variants_attempted": self.variants_attempted,
            "variants_annotated": self.variants_annotated,
            "variants_not_applicable": self.variants_not_applicable,
            "variants_failed": self.variants_failed,
            "first_failing_variant_urn": self.first_failing_variant_urn,
            "exception_class": self.exception_class,
            "message": self.message or self.skip_reason,
        }


@dataclass
class SweepTotals:
    """Corpus-level counts, so a bounded run cannot be mistaken for a complete one."""

    score_sets_published: int = 0
    score_sets_attempted: int = 0
    score_sets_skipped: int = 0
    variants_attempted: int = 0


def sweep_annotation_surface(
    score_set_urn: str,
    surface: str,
    annotate: Callable[[MappedVariant], Optional[Any]],
    mapped_variants: list[MappedVariant],
) -> SurfaceResult:
    """Attempt one annotation surface across every current mapped variant of one score set.

    Nothing raises out of here. A sweep that aborted on the first bad variant would report the corpus as
    far healthier than it is.
    """
    result = SurfaceResult(
        score_set_urn=score_set_urn,
        surface=surface,
        variants_attempted=len(mapped_variants),
    )

    for mapped_variant in mapped_variants:
        variant_urn = getattr(mapped_variant.variant, "urn", "") or ""

        try:
            annotation = annotate(mapped_variant)
        except EXPECTED_ABSENCE_EXCEPTIONS:
            # An expected absence, drawn from the same definition the streaming endpoints use so the two
            # cannot disagree. Counting it as a failure would bury real defects under millions of
            # variants that simply have nothing to annotate.
            result.variants_not_applicable += 1
            continue
        except Exception as err:
            result.record_failure(variant_urn, EXCEPTION, err)
            continue

        if annotation is None:
            # No calibration reaches this viewer, so this surface does not apply to this variant.
            result.variants_not_applicable += 1
            continue

        try:
            round_trip_annotation(annotation)
        except AnnotationRoundTripError as err:
            result.record_failure(variant_urn, SCHEMA_VIOLATION, err)
            continue
        except Exception as err:
            # The conformance check itself blew up, which is still a defect in the emitted object.
            result.record_failure(variant_urn, SCHEMA_VIOLATION, err)
            continue

        result.variants_annotated += 1

    return result


def published_score_sets(db: Session, max_score_sets: Optional[int]) -> list[ScoreSet]:
    query = select(ScoreSet).where(ScoreSet.published_date.is_not(None)).order_by(ScoreSet.urn)
    if max_score_sets is not None:
        query = query.limit(max_score_sets)
    return list(db.scalars(query).all())


def _compose_score_set_csv(
    db: Session, score_set: ScoreSet, viewer: ScoreCalibrationViewer, start: Optional[int], limit: Optional[int]
) -> str:
    """Compose the score set's CSV over every namespace discovery reports for it.

    Namespaces come from discovery rather than a fixed list, so each score set is exercised over exactly
    what it can emit — its own ClinVar releases and calibrations included — which is also what the public
    dump does.
    """
    namespaces = [entry.namespace for entry in available_score_set_csv_namespaces(db, score_set, viewer)]
    return get_score_set_variants_as_csv(
        db, score_set, namespaces, namespaced=True, start=start, limit=limit, viewer=viewer
    )


def _bisect_to_failing_row(
    db: Session, score_set: ScoreSet, viewer: ScoreCalibrationViewer, variant_count: int
) -> Optional[int]:
    """The index of the first row whose composition raises, or None if a re-run no longer fails.

    A CSV is composed for the whole score set at once, so a failure names the file rather than the row
    that caused it. Halving the window recovers the row in log2(n) recompositions and will only be
    executed after a failure has been observed.
    """
    low, high = 0, variant_count
    while low < high:
        middle = (low + high) // 2
        try:
            _compose_score_set_csv(db, score_set, viewer, start=low, limit=middle - low + 1)
        except Exception:
            high = middle
        else:
            low = middle + 1

    return low if low < variant_count else None


def sweep_csv_surface(
    db: Session, score_set: ScoreSet, score_set_urn: str, viewer: ScoreCalibrationViewer, variant_count: int
) -> SurfaceResult:
    """Compose the score set's whole variant CSV and check it reads back.

    Unlike the annotation surfaces this is one unit of work per score set, because the composer builds the
    file in a single call. It still covers every variant: the same ``variant_to_csv_row`` runs for each
    one, which is where the payload-dependent resolvers live.

    Two checks. The composition must not raise — a resolver raising from inside one cell takes the whole
    file with it, see commit 53a7b6ee. The output must read back as a rectangle of the expected size, which
    catches a value carrying a delimiter or newline that silently splits a record.
    """
    result = SurfaceResult(score_set_urn=score_set_urn, surface="score-set-csv", variants_attempted=variant_count)

    try:
        csv_text = _compose_score_set_csv(db, score_set, viewer, start=None, limit=None)
    except Exception as err:
        failing_index = _bisect_to_failing_row(db, score_set, viewer, variant_count)
        result.record_failure(_variant_urn_at(db, score_set, failing_index), EXCEPTION, err)
        result.variants_failed = 1
        return result

    parsed = list(csv.reader(StringIO(csv_text)))
    if not parsed:
        result.record_failure("", SCHEMA_VIOLATION, ValueError("composed CSV was empty, with no header row"))
        return result

    header, *data_rows = parsed
    if len(data_rows) != variant_count:
        result.record_failure(
            "",
            SCHEMA_VIOLATION,
            ValueError(f"re-parsed to {len(data_rows)} data row(s) for {variant_count} variant(s)"),
        )
        return result

    ragged = next((index for index, row in enumerate(data_rows) if len(row) != len(header)), None)
    if ragged is not None:
        result.record_failure(
            _variant_urn_at(db, score_set, ragged),
            SCHEMA_VIOLATION,
            ValueError(f"row has {len(data_rows[ragged])} field(s) against a {len(header)}-column header"),
        )
        return result

    result.variants_annotated = variant_count
    return result


def _variant_urn_at(db: Session, score_set: ScoreSet, index: Optional[int]) -> str:
    """The URN of the variant the CSV composer would place at *index*, for reporting a located failure."""
    if index is None:
        return ""

    urns = sorted(
        (urn for urn in db.scalars(select(Variant.urn).where(Variant.score_set_id == score_set.id)).all() if urn),
        key=variant_urn_sort_key,
    )
    return urns[index] if 0 <= index < len(urns) else ""


def total_variant_counts(db: Session) -> dict[Optional[int], int]:
    """Variants per score set id, for the whole corpus, in one query.

    The CSV surface is sized by this rather than by the mapped-variant count the annotation surfaces use.
    The CSV selects every variant of a score set and outer-joins its mapping, so an unmapped variant still
    gets a row with NA columns — 2309 of 2850 published score sets have more variants than current
    mappings, so conflating the two would report a false row-count violation for most of the corpus.

    Keyed by ``Optional[int]`` because ``Variant.score_set_id`` is nullable in the model. TODO(#372).
    """
    rows = db.execute(
        select(Variant.score_set_id, func.count()).select_from(Variant).group_by(Variant.score_set_id)
    ).all()
    return {score_set_id: count for score_set_id, count in rows}


@script_environment.command()
@click.option(
    "--max-score-sets",
    default=None,
    type=int,
    help="Stop after this many published score sets. The only bound available, and deliberately so: "
    "every score set the sweep reports on is swept completely, so a clean row means clean rather than "
    "clean-where-sampled. Use it to smoke-test the sweep itself.",
)
@click.option(
    "--output",
    default=None,
    help="CSV path. Defaults to export-sweep.YYYYMMDDHHMMSS.csv in the working directory.",
)
@with_database_session
def export_sweep(db: Session, max_score_sets: Optional[int], output: Optional[str]):
    output_path = output or f"export-sweep.{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"

    # Matches the public data export: publishing a score set does not publish its calibrations.
    principal = Principal()
    viewer = principal.viewer_for(ScoreCalibrationViewer)
    surfaces = _surfaces(principal)

    score_sets = published_score_sets(db, max_score_sets)
    variant_counts = total_variant_counts(db)
    totals = SweepTotals(score_sets_published=len(score_sets))

    logger.info(
        f"Sweeping {len(score_sets)} published score sets across {len(surfaces) + 1} surfaces, "
        "attempting every current mapped variant of each."
    )
    if max_score_sets is not None:
        logger.warning(f"Bounded to the first {max_score_sets} score sets by --max-score-sets; not full coverage.")

    rows: list[dict[str, Any]] = []

    for index, score_set in enumerate(score_sets):
        urn = score_set.urn or f"<score set id {score_set.id}>"
        mapped_variants = list(get_current_mapped_variants_for_annotation(db, score_set))
        variant_count = variant_counts.get(score_set.id, 0)
        surface_results: list[SurfaceResult] = []

        # The annotation surfaces need a current mapping to have anything to say. The CSV surface does
        # not: it emits a row per variant and outer-joins the mapping, so an unmapped variant still gets
        # a row of NA columns. The two skip on different conditions for that reason.
        if mapped_variants:
            surface_results.extend(
                sweep_annotation_surface(urn, surface, annotate, mapped_variants) for surface, annotate in surfaces
            )
        else:
            surface_results.extend(
                SurfaceResult(
                    score_set_urn=urn, surface=surface, outcome=SKIPPED, skip_reason="no current mapped variants"
                )
                for surface, _ in surfaces
            )

        if variant_count:
            surface_results.append(sweep_csv_surface(db, score_set, urn, viewer, variant_count))
        else:
            surface_results.append(
                SurfaceResult(score_set_urn=urn, surface="score-set-csv", outcome=SKIPPED, skip_reason="no variants")
            )

        if mapped_variants or variant_count:
            totals.score_sets_attempted += 1
            totals.variants_attempted += len(mapped_variants)
        else:
            totals.score_sets_skipped += 1

        for result in surface_results:
            rows.append(result.as_row())

            if result.outcome in FAILURE_OUTCOMES:
                logger.error(
                    f"{urn} / {result.surface}: {result.outcome} on {result.variants_failed} of "
                    f"{result.variants_attempted} variant(s); first was "
                    f"{result.first_failing_variant_urn} ({result.exception_class}: {result.message})"
                )

        # The corpus is large enough that a silent run looks like a hung one.
        if (index + 1) % 100 == 0:
            logger.info(f"[{index + 1}/{len(score_sets)}] swept")

        # Each score set's variants are only needed while its surfaces are attempted. Without this the
        # session accumulates the whole corpus.
        db.expunge_all()

    with open(output_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    # Counted off the emitted rows so the summary cannot disagree with the CSV it describes.
    outcomes = Counter(row["outcome"] for row in rows)
    failures = sum(outcomes[outcome] for outcome in FAILURE_OUTCOMES)

    logger.info(f"Wrote {len(rows)} rows to {output_path}")
    logger.info(
        f"Score sets: {totals.score_sets_published} published, {totals.score_sets_attempted} attempted, "
        f"{totals.score_sets_skipped} skipped with no current mapped variants."
    )
    logger.info(f"Variants: {totals.variants_attempted} attempted per surface, every one held by those score sets.")
    logger.info("Outcomes: " + ", ".join(f"{outcome}={count}" for outcome, count in sorted(outcomes.items())))

    if failures:
        logger.error(f"{failures} (score set, surface) pair(s) failed. See {output_path}.")
        sys.exit(1)

    logger.info("Every attempted surface annotated and round-tripped.")


if __name__ == "__main__":
    export_sweep()
