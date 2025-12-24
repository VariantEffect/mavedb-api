"""Load an archive of Zeiberg calibration style calibrations into Score Sets.

This script processes JSON calibration files from an archive directory and applies them
to MaveDB Score Sets based on a dataset mapping file. The script iterates through all
JSON files in the archive directory, extracts dataset names from filenames, looks up
corresponding Score Set URNs in the mapping file, and creates ACMG-style functional
range calibrations for each Score Set.

Args:
    archive_path (str): Path to directory containing calibration JSON files
    dataset_map (str): Path to JSON file mapping dataset names to Score Set URNs
    overwrite (bool): Whether to overwrite existing "Zeiberg calibration" entries

Input File Formats:

1. Archive Directory Structure:
   - Contains JSON files named after datasets (e.g., "data_set_name.json")
   - May include "_clinvar_2018" variant files (e.g., "data_set_name_clinvar_2018.json")
   - Script automatically detects and processes both variants

2. Calibration JSON File Format:
   {
     "prior": 0.01548246603645654,
     "point_ranges": {
       "1": [[[0.7222, 0.9017]]],     // BS3 Supporting (-1 to 1 points)
       "2": [[[0.9017, 1.1315]]],     // BS3 Moderate (-2 to 2 points)
       "3": [[[1.1315, 5.3892]]],     // BS3 Moderate+ (-3 to 3 points)
       "4": [],                        // BS3 Strong (-4 to 4 points)
       "-1": [[[-0.6934, -0.3990]]],  // PS3 Supporting
       "-2": [[[-6.5761, -0.6934]]],  // PS3 Moderate
       // ... other point values (-8 to 8)
     },
     "dataset": "data_set_name",
     "relax": false,
     "n_c": "2c",
     "benign_method": "benign",
     "clinvar_2018": false
   }

3. Dataset Mapping JSON File Format:
   {
     "data_set_name": "urn:mavedb:00000050-a-1",
     "data_set_with_urn_list": "urn:mavedb:00000060-a-1, urn:mavedb:00000060-a-2",
     // ... more dataset mappings
   }

Behavior:

1. File Discovery: Scans archive directory for all .json files
2. Dataset Name Extraction: Removes .json extension and _clinvar_2018 suffix
3. Mapping Lookup: Finds Score Set URNs for each dataset in mapping file
4. URN Processing: Handles comma-separated URN lists for datasets with multiple Score Sets
5. Calibration Creation: Creates functional ranges with ACMG classifications:
   - Positive points (1-8): PS3 classifications for "abnormal" variants
   - Negative points (-1 to -8): BS3 classifications for "normal" variants
   - Strength labels: Supporting (±1), Moderate (±2), Moderate+ (±3), Strong (±4-8)
6. File Variants: Automatically detects and processes both regular and ClinVar 2018 variants
7. Calibration Naming:
   - Regular files: "Zeiberg calibration"
   - ClinVar 2018 files: "Zeiberg calibration (ClinVar 2018)"

Skipping Behavior:
- Files with no mapping entry or empty/invalid URNs (N/A, #VALUE!, empty string)
- Score Sets that don't exist in the database
- JSON files that can't be parsed

Output Statistics:
- Total JSON files found in archive
- Number of calibrations created vs updated
- Number of unmapped files
- Number of non-existing Score Sets

Example Usage:
    python load_pp_style_calibration.py /path/to/calibrations_archive /path/to/dataset_mapping.json
    python load_pp_style_calibration.py /path/to/calibrations_archive /path/to/dataset_mapping.json --overwrite
"""

import asyncio
import json
import os
from typing import Dict, List, Optional

import click
from sqlalchemy.orm import Session

from mavedb.lib.score_calibrations import create_score_calibration_in_score_set
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
ZEIBERG_CALIBRATION_CITATION = {"identifier": "2025.04.29.651326", "db_name": "bioRxiv"}


@click.command()
@with_database_session
@click.argument("archive_path", type=click.Path(exists=True, file_okay=False))
@click.argument("dataset_map", type=click.Path(exists=True, dir_okay=False))
@click.option("--overwrite", is_flag=True, default=False, help="Overwrite existing `Zeiberg calibration` in score set")
def main(db: Session, archive_path: str, dataset_map: str, overwrite: bool) -> None:
    """Load an archive of Zeiberg calibration style calibrations into Score Sets"""
    with open(dataset_map, "r") as f:
        dataset_mapping: Dict[str, str] = json.load(f)

    system_user: User = db.query(User).filter(User.id == 1).one()

    # Get all JSON files in the archive directory
    json_files = [f for f in os.listdir(archive_path) if f.endswith(".json")]
    total_json_files = len(json_files)

    created_calibrations = 0
    updated_calibrations = 0
    non_existing_score_sets = 0
    unmapped_files = []

    click.echo(f"Found {total_json_files} JSON files in archive directory: {archive_path}")

    for json_file in json_files:
        with open(os.path.join(archive_path, json_file), "r") as f:
            calibration_data = json.load(f)
            dataset_name = calibration_data.get("dataset", None)
            click.echo(f"Processing calibration file: {json_file} (dataset: {dataset_name})")

        if not dataset_name:
            click.echo(f"   Dataset name not found in calibration file {json_file}, skipping...", err=True)
            unmapped_files.append(json_file)
            continue

        # Look up dataset in mapping
        if "_clinvar_2018" in json_file:
            dataset_name = dataset_name.replace("_clinvar_2018", "")

        score_set_urns_str = dataset_mapping.get(dataset_name)
        if not score_set_urns_str or score_set_urns_str in ["", "N/A", "#VALUE!"]:
            click.echo(f"   Dataset {dataset_name} not found in mapping or has no URNs, skipping...", err=True)
            unmapped_files.append(json_file)
            continue

        # Handle comma-separated list of score set URNs
        score_set_urns = [urn.strip() for urn in score_set_urns_str.split(",") if urn.strip()]

        # Process each score set URN for this calibration file
        for score_set_urn in score_set_urns:
            click.echo(f"   Applying calibration to Score Set {score_set_urn}...")

            score_set: Optional[ScoreSet] = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()
            if not score_set:
                click.echo(f"      Score Set with URN {score_set_urn} not found, skipping...", err=True)
                non_existing_score_sets += 1
                continue

            # Determine calibration name based on file name
            if "_clinvar_2018" in json_file:
                calibration_name = "Zeiberg calibration (ClinVar 2018)"
            else:
                calibration_name = "Zeiberg calibration"

            existing_calibration = None
            if overwrite:
                existing_calibration = (
                    db.query(ScoreCalibration)
                    .filter(ScoreCalibration.score_set_id == score_set.id)
                    .filter(ScoreCalibration.title == calibration_name)
                    .one_or_none()
                )

                if existing_calibration:
                    db.delete(existing_calibration)
                    db.flush()
                    click.echo(f"      Overwriting existing '{calibration_name}' in Score Set {score_set.urn}")

            benign_has_lower_functional_scores = calibration_data.get("scoreset_flipped", False)
            functional_ranges: List[score_calibration.FunctionalRangeCreate] = []
            for points, range_data in calibration_data.get("point_ranges", {}).items():
                if not range_data:
                    continue

                lower_bound, upper_bound = range_data[0][0], range_data[0][1]

                if lower_bound == float("-inf"):
                    lower_bound = None
                if upper_bound == float("inf"):
                    upper_bound = None

                range_data = (lower_bound, upper_bound)
                points = int(points.strip())
                ps_or_bs = "PS3" if points > 0 else "BS3"
                strength_label = ALL_POINT_LABEL_MAPPINGS.get(points, "Unknown")

                # The boundary of the functional range closest to the implied indeterminate range
                # will always be non-inclusive, as we assign any variants with this score to the
                # lowest points value.
                if (benign_has_lower_functional_scores and points < 0) or (
                    not benign_has_lower_functional_scores and points > 0
                ):
                    inclusive_lower = True if lower_bound is not None else False
                    inclusive_upper = False
                else:
                    inclusive_lower = False
                    inclusive_upper = True if upper_bound is not None else False

                functional_range = score_calibration.FunctionalRangeCreate(
                    label=f"{ps_or_bs} {strength_label} ({points})",
                    classification="abnormal" if points > 0 else "normal",
                    range=range_data,
                    acmg_classification=acmg_classification.ACMGClassificationCreate(
                        points=int(points),
                    ),
                    inclusive_lower_bound=inclusive_lower,
                    inclusive_upper_bound=inclusive_upper,
                )
                functional_ranges.append(functional_range)

            score_calibration_create = score_calibration.ScoreCalibrationCreate(
                title=calibration_name,
                functional_ranges=functional_ranges,
                research_use_only=True,
                score_set_urn=score_set.urn,
                calibration_metadata={"prior_probability_pathogenicity": calibration_data.get("prior", None)},
                method_sources=[ZEIBERG_CALIBRATION_CITATION],
                threshold_sources=[],
                classification_sources=[],
            )

            new_calibration_object = asyncio.run(
                create_score_calibration_in_score_set(db, score_calibration_create, system_user)
            )
            new_calibration_object.primary = False
            new_calibration_object.private = False
            db.add(new_calibration_object)

            click.echo(f"      Successfully created calibration '{calibration_name}' for Score Set {score_set.urn}")
            db.flush()

            if existing_calibration:
                updated_calibrations += 1
            else:
                created_calibrations += 1

    click.echo(
        "---\n"
        f"Created {created_calibrations} calibrations, updated {updated_calibrations} calibrations ({created_calibrations + updated_calibrations} total). Non-existing score sets: {non_existing_score_sets}."
    )
    click.echo(
        f"{len(unmapped_files)} unmapped calibration files out of {total_json_files} files in archive. Unmapped files were:"
    )
    for unmapped_file in unmapped_files:
        click.echo(f"  - {unmapped_file}")


if __name__ == "__main__":  # pragma: no cover
    main()
