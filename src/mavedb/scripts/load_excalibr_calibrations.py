"""Load ExCALIBR calibrations from a CSV file into Score Sets.

This script processes a CSV file containing ExCALIBR calibration data based on
the format provided in the publications supplementary materials and applies
calibrations to MaveDB Score Sets based on a dataset mapping file.

Args:
    csv_path (str): Path to the CSV file containing calibration data
    dataset_map (str): Path to JSON file mapping dataset names to Score Set URNs
    overwrite (bool): Whether to overwrite existing "ExCALIBR calibration" entries

Input File Formats:

1. CSV File Format:
   Columns:
     - dataset: Dataset name (used to look up Score Set URNs in mapping file)
     - prior: Prior probability of pathogenicity
     - range_-8 through range_8 (excluding 0): Space-separated "lower upper" bound
       pairs for each ACMG point level. Empty cells indicate no range at that level.
       Values may include "inf" and "-inf" for unbounded ranges.
     - relax: Whether calibration uses relaxed thresholds (TRUE/FALSE)
     - n_c: Number of classes ("2c" or "3c")
     - benign_method: Method used for benign classification
     - clinvar_2018: Whether this is a ClinVar 2018 variant calibration (TRUE/FALSE)
     - scoreset_flipped: Whether benign variants have lower functional scores (TRUE/FALSE)

2. Dataset Mapping JSON File Format:
   {
     "data_set_name": "urn:mavedb:00000050-a-1",
     "data_set_with_urn_list": "urn:mavedb:00000060-a-1, urn:mavedb:00000060-a-2",
     // ... more dataset mappings
   }

Calibration Naming:
   - Regular: "ExCALIBR calibration"
   - ClinVar 2018: "ExCALIBR calibration (ClinVar 2018)"

Example Usage:
    python load_excalibr_calibrations_csv.py /path/to/calibrations.csv /path/to/dataset_mapping.json
    python load_excalibr_calibrations_csv.py /path/to/calibrations.csv /path/to/dataset_mapping.json --overwrite
"""

import asyncio
import csv
import json
from typing import Dict, List, Optional, Tuple

import click
from sqlalchemy.orm import Session

from mavedb.lib.score_calibrations import create_score_calibration_in_score_set
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.scripts.environment import with_database_session
from mavedb.view_models import acmg_classification, score_calibration

POINT_LABEL_MAPPINGS: Dict[int, str] = {
    8: "Very Strong",
    7: "Strong",
    6: "Strong",
    5: "Strong",
    4: "Strong",
    3: "Moderate+",
    2: "Moderate",
    1: "Supporting",
}

ALL_POINT_LABEL_MAPPINGS = {**POINT_LABEL_MAPPINGS, **{k * -1: v for k, v in POINT_LABEL_MAPPINGS.items()}}
EXCALIBR_CALIBRATION_CITATION = {"identifier": "2025.04.29.651326", "db_name": "bioRxiv"}

# Point levels corresponding to CSV range columns (range_-8 through range_8, excluding 0)
POINT_LEVELS = [-8, -7, -6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8]


def parse_range_value(value: str) -> Optional[Tuple[Optional[float], Optional[float]]]:
    """Parse a space-separated range value from a CSV cell into a (lower, upper) tuple.

    Values of "inf" and "-inf" are converted to None to represent unbounded ranges.
    Returns None if the cell is empty (no range at this point level).
    """
    value = value.strip()
    if not value:
        return None

    parts = value.split()
    if len(parts) != 2:
        return None

    lower_str, upper_str = parts

    lower: Optional[float] = float(lower_str)
    upper: Optional[float] = float(upper_str)

    if lower == float("-inf"):
        lower = None
    if upper == float("inf"):
        upper = None

    return (lower, upper)


@click.command()
@with_database_session
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("dataset_map", type=click.Path(exists=True, dir_okay=False))
@click.option("--overwrite", is_flag=True, default=False, help="Overwrite existing `ExCALIBR calibration` in score set")
@click.option(
    "--remove",
    is_flag=True,
    default=False,
    help="Remove any ExCALIBR calibrations not loaded by this run",
)
def main(db: Session, csv_path: str, dataset_map: str, overwrite: bool, remove: bool) -> None:
    """Load ExCALIBR calibrations from a CSV file into Score Sets."""
    with open(dataset_map, "r") as f:
        dataset_mapping: Dict[str, str] = json.load(f)

    system_user: User = db.query(User).filter(User.id == 1).one()

    created_calibrations = 0
    updated_calibrations = 0
    non_existing_score_sets = 0
    unmapped_rows = []
    total_rows = 0
    loaded_calibration_ids: set[int] = set()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1
            dataset_name = row.get("dataset", "").strip()
            is_clinvar_2018 = row.get("clinvar_2018", "").strip().upper() == "TRUE"
            scoreset_flipped = row.get("scoreset_flipped", "").strip().upper() == "TRUE"
            prior_str = row.get("prior", "").strip()
            prior = float(prior_str) if prior_str else None

            click.echo(f"Processing row {total_rows}: dataset={dataset_name}, clinvar_2018={is_clinvar_2018}")

            if not dataset_name:
                click.echo("   Dataset name is empty, skipping...", err=True)
                unmapped_rows.append(f"row {total_rows} (empty dataset)")
                continue

            # Strip _clinvar_2018 suffix from dataset name for mapping lookup
            mapping_key = dataset_name.replace("_clinvar_2018", "") if is_clinvar_2018 else dataset_name

            score_set_urns_str = dataset_mapping.get(mapping_key)
            if not score_set_urns_str or score_set_urns_str in ["", "N/A", "#VALUE!"]:
                click.echo(f"   Dataset {mapping_key} not found in mapping or has no URNs, skipping...", err=True)
                unmapped_rows.append(f"row {total_rows} ({dataset_name})")
                continue

            # Handle comma-separated list of score set URNs
            score_set_urns = [urn.strip() for urn in score_set_urns_str.split(",") if urn.strip()]

            # Parse ranges from CSV columns
            point_ranges: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
            for points in POINT_LEVELS:
                col_name = f"range_{points}"
                range_value = row.get(col_name, "").strip()
                parsed = parse_range_value(range_value)
                if parsed is not None:
                    point_ranges[points] = parsed

            # Process each score set URN for this row
            for score_set_urn in score_set_urns:
                click.echo(f"   Applying calibration to Score Set {score_set_urn}...")

                score_set: Optional[ScoreSet] = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()
                if not score_set:
                    click.echo(f"      Score Set with URN {score_set_urn} not found, skipping...", err=True)
                    non_existing_score_sets += 1
                    continue

                if is_clinvar_2018:
                    calibration_name = "ExCALIBR calibration (ClinVar 2018)"
                    legacy_name = "Zeiberg calibration (ClinVar 2018)"
                else:
                    calibration_name = "ExCALIBR calibration"
                    legacy_name = "Zeiberg calibration"

                existing_calibration = None
                if overwrite:
                    existing_calibration = (
                        db.query(ScoreCalibration)
                        .filter(ScoreCalibration.score_set_id == score_set.id)
                        .filter(ScoreCalibration.title.in_([calibration_name, legacy_name]))
                        .one_or_none()
                    )

                    if existing_calibration:
                        db.delete(existing_calibration)
                        db.flush()
                        click.echo(f"      Overwriting existing '{calibration_name}' in Score Set {score_set.urn}")

                functional_classifications: List[score_calibration.FunctionalClassificationCreate] = []
                for points, range_data in point_ranges.items():
                    lower_bound, upper_bound = range_data

                    ps_or_bs = "PS3" if points > 0 else "BS3"
                    strength_label = ALL_POINT_LABEL_MAPPINGS.get(points, "Unknown")

                    # The boundary of the functional range closest to the implied indeterminate range
                    # will always be non-inclusive, as we assign any variants with this score to the
                    # lowest points value.
                    if (scoreset_flipped and points < 0) or (not scoreset_flipped and points > 0):
                        inclusive_lower = True if lower_bound is not None else False
                        inclusive_upper = False
                    else:
                        inclusive_lower = False
                        inclusive_upper = True if upper_bound is not None else False

                    functional_range = score_calibration.FunctionalClassificationCreate(
                        label=f"{ps_or_bs} {strength_label} ({points})",
                        functional_classification=FunctionalClassificationOptions.abnormal
                        if points > 0
                        else FunctionalClassificationOptions.normal,
                        range=range_data,
                        acmg_classification=acmg_classification.ACMGClassificationCreate(
                            points=int(points),
                        ),
                        inclusive_lower_bound=inclusive_lower,
                        inclusive_upper_bound=inclusive_upper,
                    )
                    functional_classifications.append(functional_range)

                score_calibration_create = score_calibration.ScoreCalibrationCreate(
                    title=calibration_name,
                    functional_classifications=functional_classifications,
                    research_use_only=True,
                    score_set_urn=score_set.urn,
                    calibration_metadata={"prior_probability_pathogenicity": prior},
                    threshold_sources=[EXCALIBR_CALIBRATION_CITATION],
                    evidence_sources=[EXCALIBR_CALIBRATION_CITATION],
                    method_sources=[EXCALIBR_CALIBRATION_CITATION],
                )

                new_calibration_object = asyncio.run(
                    create_score_calibration_in_score_set(db, score_calibration_create, system_user)
                )
                new_calibration_object.primary = False
                new_calibration_object.private = False
                db.add(new_calibration_object)

                click.echo(f"      Successfully created calibration '{calibration_name}' for Score Set {score_set.urn}")
                db.flush()
                loaded_calibration_ids.add(new_calibration_object.id)  # type: ignore

                if existing_calibration:
                    updated_calibrations += 1
                else:
                    created_calibrations += 1

    click.echo(
        "---\n"
        f"Processed {total_rows} CSV rows. "
        f"Created {created_calibrations} calibrations, updated {updated_calibrations} calibrations "
        f"({created_calibrations + updated_calibrations} total). "
        f"Non-existing score sets: {non_existing_score_sets}."
    )
    if unmapped_rows:
        click.echo(f"{len(unmapped_rows)} unmapped rows out of {total_rows} rows:")
        for unmapped in unmapped_rows:
            click.echo(f"  - {unmapped}")

    if remove:
        excalibr_titles = [
            "ExCALIBR calibration",
            "ExCALIBR calibration (ClinVar 2018)",
            "Zeiberg calibration",
            "Zeiberg calibration (ClinVar 2018)",
        ]

        stale_query = db.query(ScoreCalibration).filter(ScoreCalibration.title.in_(excalibr_titles))
        if loaded_calibration_ids:
            stale_query = stale_query.filter(ScoreCalibration.id.notin_(loaded_calibration_ids))

        stale_calibrations = stale_query.all()

        if stale_calibrations:
            click.echo(f"\nRemoving {len(stale_calibrations)} ExCALIBR calibration(s) not loaded by this run:")
            for cal in stale_calibrations:
                click.echo(f"  - '{cal.title}' on Score Set {cal.score_set.urn} (id={cal.id})")
                db.delete(cal)
            db.flush()
        else:
            click.echo("\nNo stale ExCALIBR calibrations to remove.")


if __name__ == "__main__":  # pragma: no cover
    main()
