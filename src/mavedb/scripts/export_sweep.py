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

Every current mapped variant of every published score set is attempted. There is deliberately no
per-score-set sampling: a sampled sweep can only ever report a score set as unbroken-where-sampled,
and the measured cost of doing all of it is well under an hour, which is the right order for a
pre-release or periodic check.

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
from typing import Any, Callable, Optional

import asyncclick as click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.conformance import AnnotationRoundTripError, round_trip_annotation
from mavedb.lib.annotation.exceptions import EXPECTED_ABSENCE_EXCEPTIONS
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.score_sets import get_current_mapped_variants_for_annotation
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
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


def sweep_surface(
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
    surfaces = _surfaces(principal)

    score_sets = published_score_sets(db, max_score_sets)
    totals = SweepTotals(score_sets_published=len(score_sets))

    logger.info(
        f"Sweeping {len(score_sets)} published score sets across {len(surfaces)} surfaces, "
        "attempting every current mapped variant of each."
    )
    if max_score_sets is not None:
        logger.warning(f"Bounded to the first {max_score_sets} score sets by --max-score-sets; not full coverage.")

    rows: list[dict[str, Any]] = []

    for index, score_set in enumerate(score_sets):
        urn = score_set.urn or f"<score set id {score_set.id}>"
        mapped_variants = list(get_current_mapped_variants_for_annotation(db, score_set))

        if not mapped_variants:
            totals.score_sets_skipped += 1
            for surface, _ in surfaces:
                skipped = SurfaceResult(
                    score_set_urn=urn,
                    surface=surface,
                    outcome=SKIPPED,
                    skip_reason="no current mapped variants",
                )
                rows.append(skipped.as_row())
            continue

        totals.score_sets_attempted += 1
        totals.variants_attempted += len(mapped_variants)

        for surface, annotate in surfaces:
            result = sweep_surface(urn, surface, annotate, mapped_variants)
            rows.append(result.as_row())

            if result.outcome in FAILURE_OUTCOMES:
                logger.error(
                    f"{urn} / {surface}: {result.outcome} on {result.variants_failed} of "
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
