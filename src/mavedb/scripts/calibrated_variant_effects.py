"""
Count unique variant effect measurements within ACMG-classified functional ranges.

This script analyzes MaveDB score sets to count how many variant effect measurements
have functional scores that fall within score calibration ranges associated with
ACMG (American College of Medical Genetics) classifications. The analysis provides
insights into how many variants can be clinically interpreted using established
evidence strength frameworks.

Usage:
    # Show help and available options
    with_mavedb_local poetry run python3 -m mavedb.scripts.effect_measurements --help

    # Run in dry-run mode (default, no database changes, shows results)
    with_mavedb_local poetry run python3 -m mavedb.scripts.effect_measurements --dry-run

    # Run and commit results (this script is read-only, so commit doesn't change anything)
    with_mavedb_local poetry run python3 -m mavedb.scripts.effect_measurements --commit

Behavior:
    1. Queries all non-superseded score sets that have score calibrations
    2. Identifies calibrations with functional ranges that have ACMG classifications
    3. For each qualifying score set, queries its variants with non-null scores
    4. Counts variants whose scores fall within ACMG-classified ranges
    5. Reports statistics on classification coverage

Key Filters:
    - Excludes superseded score sets (where superseding_score_set is not None)
    - Only processes score sets that have at least one score calibration
    - Only considers functional ranges with ACMG classification data
    - Only counts variants that have non-null functional scores
    - Each variant is counted only once per score set, even if it matches multiple ranges

ACMG Classification Detection:
    A functional range is considered to have an ACMG classification if its
    acmg_classification field contains any of:
    - criterion (PS3, BS3, etc.)
    - evidence_strength (Supporting, Moderate, Strong, Very Strong)
    - points (numeric evidence points)

Performance Notes:
    - Uses optimized queries to avoid loading unnecessary data
    - Loads score sets and calibrations first, then queries variants separately
    - Filters variants at the database level for better performance
    - Memory usage scales with the number of score sets with ACMG ranges

Output:
    - Progress updates for each score set with classified variants
    - Summary statistics including:
      * Number of score sets with ACMG classifications
      * Total unique variants processed
      * Number of variants within ACMG-classified ranges
      * Overall classification rate percentage

Caveats:
    - This is a read-only analysis script (makes no database changes)
    - Variants with null/missing scores are included in the analysis
"""

import logging
from typing import Set

import click
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.scripts.environment import with_database_session

logger = logging.getLogger(__name__)


@click.command()
@with_database_session
def main(db: Session) -> None:
    """Count unique variant effect measurements with ACMG-classified functional ranges."""

    query = (
        select(ScoreSet)
        .options(joinedload(ScoreSet.score_calibrations))
        .where(ScoreSet.private.is_(False))  # Public score sets only
        .where(ScoreSet.superseded_score_set_id.is_(None))  # Not superseded
        .where(ScoreSet.score_calibrations.any())  # Has calibrations
    )

    score_sets = db.scalars(query).unique().all()

    total_variants_count = 0
    classified_variants_count = 0
    score_sets_with_acmg_count = 0
    gene_list: Set[str] = set()

    click.echo(f"Found {len(score_sets)} non-superseded score sets with calibrations")

    for score_set in score_sets:
        # Collect all ACMG-classified ranges from this score set's calibrations
        acmg_ranges: list[ScoreCalibrationFunctionalClassification] = []
        for calibration in score_set.score_calibrations:
            if calibration.functional_classifications:
                for func_classification in calibration.functional_classifications:
                    if func_classification.acmg_classification_id is not None:
                        acmg_ranges.append(func_classification)

        if not acmg_ranges:
            continue

        score_sets_with_acmg_count += 1

        # Retain a list of unique target genes for reporting
        for target in score_set.target_genes:
            target_name = target.name
            if not target_name:
                continue

            gene_list.add(target_name.strip().upper())

        score_set_classified_variants: set[int] = set()
        for classified_range in acmg_ranges:
            variants_classified_by_range: list[int] = [
                variant.id for variant in classified_range.variants if variant.id is not None
            ]
            score_set_classified_variants.update(variants_classified_by_range)

        total_variants_count += score_set.num_variants or 0
        classified_variants_count += len(score_set_classified_variants)
        if score_set_classified_variants:
            click.echo(
                f"Score set {score_set.urn}: {len(score_set_classified_variants)} classified variants ({score_set.num_variants} total variants)"
            )

    click.echo("\n" + "=" * 60)
    click.echo("SUMMARY")
    click.echo("=" * 60)
    click.echo(f"Score sets with ACMG classifications: {score_sets_with_acmg_count}")
    click.echo(f"Total unique variants processed: {total_variants_count}")
    click.echo(f"Variants within ACMG-classified ranges: {classified_variants_count}")
    click.echo(f"Unique target genes covered ({len(gene_list)}):")
    for gene in sorted(gene_list):
        click.echo(f" - {gene}")

    if total_variants_count > 0:
        percentage = (classified_variants_count / total_variants_count) * 100
        click.echo(f"Classification rate: {percentage:.1f}%")


if __name__ == "__main__":  # pragma: no cover
    main()
