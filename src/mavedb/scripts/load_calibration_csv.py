"""
This script loads calibration data from a CSV file into the database.

CSV Format:
The CSV file must contain the following columns with their expected data types and formats:

Core Metadata Columns:
- score_set_urn: The URN identifier for the score set (e.g., "urn:mavedb:00000657-a-1"). Can contain multiple URNs separated by commas.
- pp_data_set_tag: Tag identifying the PP data set (e.g., "ASPA_Grønbæk-Thygesen_2024_abundance").
- calibration_name: Name of the calibration method (e.g., "investigator_provided", "cvfg_missense_vars", "cvfg_all_vars").
- primary: Boolean value indicating if this is the primary calibration (TRUE/FALSE).
- calibration_notes_for_mavedb: Notes specific to MaveDB about this calibration (text, can be empty).
- notes: General notes about the calibration (text, can be empty).
- target_type: Type of target being analyzed (e.g., "synthetic", "endogenous").
- calibration_notes: Additional calibration notes (text, can be empty).
- cite_brnich_method: Boolean indicating if Brnich method was cited (TRUE/FALSE).
- thresholds_pmid: PubMed ID for threshold methodology (numeric, can be empty).
- odds_path_pmid: PubMed ID for odds path methodology (e.g., "cvfg", numeric PMID, can be empty).

Baseline Score Information:
- baseline_score: The baseline score value used for normalization (numeric, can be empty).
- baseline_score_notes: Additional notes about the baseline score (text, can be empty).

Classification Class Columns (classes 1-5, following consistent naming pattern):
Class 1:
- class_1_range: The range for the first class (e.g., "(-Inf, 0.2)", "[-0.748, Inf)").
- class_1_name: The name/label for the first class (e.g., "low abundance", "Functional").
- class_1_functional_classification: The functional classification (e.g., "abnormal", "normal", "indeterminate").
- class_1_odds_path: The odds path value for the first class (numeric, can be empty).
- class_1_strength: The strength of evidence (e.g., "PS3_MODERATE", "BS3_STRONG", can be empty).

...

Class 5:
- class_5: The range for the fifth class.
- class_5_name: The name/label for the fifth class.
- class_5_functional_classification: The functional classification for the fifth class.
- class_5_odds_path: The odds path value for the fifth class (numeric, can be empty).
- class_5_strength: The strength of evidence for the fifth class (can be empty).

Usage:
This script loads calibration data from a CSV file into the database, creating score calibrations
for score sets based on the provided functional class ranges and evidence strengths.

Command Line Interface:
The script uses Click for command-line argument parsing and requires a database session.

Arguments:
- csv_path: Path to the input CSV file (required). Must exist and be readable.

Options:
- --delimiter: CSV delimiter character (default: ",")
- --overwrite: Flag to overwrite existing calibration containers for each score set (default: False)
- --purge-publication-relationships: Flag to purge existing publication relationships (default: False)

Behavior:
- Processes each row in the CSV file and creates score calibrations for the specified score sets
- Skips rows without valid URNs or functional class ranges
- Only replaces the targeted container key unless --overwrite is specified
- Uses the calibration_name field to determine the container key for the calibration
- Supports multiple URNs per row (comma-separated in the score_set_urn column)
- Automatically handles database session management through the @with_database_session decorator

Example usage:
```bash
# Basic usage with default comma delimiter
python load_calibration_csv.py /path/to/calibration_data.csv

# Use a different delimiter (e.g., semicolon)
python load_calibration_csv.py /path/to/calibration_data.csv --delimiter ";"

# Overwrite existing calibration containers
python load_calibration_csv.py /path/to/calibration_data.csv --overwrite

# Purge existing publication relationships before loading
python load_calibration_csv.py /path/to/calibration_data.csv --purge-publication-relationships

# Combine multiple options
python load_calibration_csv.py /path/to/calibration_data.csv --delimiter ";" --overwrite --purge-publication-relationships
```

Exit Behavior:
The script will output summary statistics showing:
- Number of score sets updated
- Number of rows skipped (due to missing URNs or invalid ranges)
- Number of errors encountered
- Total number of rows processed

"""

import asyncio
import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import click
from sqlalchemy.orm import Session

from mavedb.lib.acmg import ACMGCriterion, StrengthOfEvidenceProvided
from mavedb.lib.oddspaths import oddspaths_evidence_strength_equivalent
from mavedb.lib.score_calibrations import create_score_calibration_in_score_set
from mavedb.models import score_calibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.scripts.environment import with_database_session
from mavedb.view_models.acmg_classification import ACMGClassificationCreate
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_calibration import FunctionalClassificationCreate, ScoreCalibrationCreate

BRNICH_PMID = "31892348"
RANGE_PATTERN = re.compile(r"^\s*([\[(])\s*([^,]+)\s*,\s*([^\])]+)\s*([])])\s*$", re.IGNORECASE)
INFINITY_TOKENS = {"inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"}
MAX_RANGES = 5

NAME_ALIASES = {
    "investigator_provided": "Investigator-provided functional classes",
    "scott": "Scott calibration",
    "cvfg_all_vars": "IGVF Coding Variant Focus Group -- Controls: All Variants",
    "cvfg_missense_vars": "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only",
    "fayer": "Fayer calibration",
}


def parse_bound(raw: str) -> Optional[float]:
    raw = raw.strip()
    if not raw:
        return None
    rl = raw.lower()
    if rl in INFINITY_TOKENS:
        return None
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"Unparseable bound '{raw}'")


def parse_interval(text: str) -> Tuple[Optional[float], Optional[float], bool, bool]:
    m = RANGE_PATTERN.match(text)
    if not m:
        raise ValueError(f"Invalid range format '{text}'")
    left_br, lower_raw, upper_raw, right_br = m.groups()
    lower = parse_bound(lower_raw)
    upper = parse_bound(upper_raw)
    inclusive_lower = left_br == "["
    inclusive_upper = right_br == "]"
    if lower is not None and upper is not None:
        if lower > upper:
            raise ValueError("Lower bound greater than upper bound")
        if lower == upper:
            raise ValueError("Lower bound equals upper bound")
    return lower, upper, inclusive_lower, inclusive_upper


def normalize_classification(
    raw: Optional[str], strength: Optional[str]
) -> Literal["normal", "abnormal", "not_specified"]:
    if raw:
        r = raw.strip().lower()
        if r in {"normal", "abnormal", "not_specified"}:
            return r  # type: ignore[return-value]
        if r in {"indeterminate", "uncertain", "unknown"}:
            return "not_specified"

    if strength:
        if strength.upper().startswith("PS"):
            return "abnormal"
        if strength.upper().startswith("BS"):
            return "normal"

    return "not_specified"


def build_publications(
    cite_brnich: str, thresholds_pmid: str, oddspaths_pmid: str, calculation_pmid: str
) -> tuple[List[PublicationIdentifierCreate], List[PublicationIdentifierCreate], List[PublicationIdentifierCreate]]:
    """Return (source_publications, oddspaths_publications).

    Rules:
      - Brnich citation only goes to source when cite_brnich_method == TRUE.
      - thresholds_pmid (if present) -> source only.
      - oddspaths_pmid (if present) -> oddspaths_source only.
      - calculation_pmid (if present) -> calculation_source only.
      - Duplicates between lists preserved separately if same PMID used for both roles.
    """
    threshold_pmids: set[str] = set()
    method_pmids: set[str] = set()
    calculation_pmids: set[str] = set()

    if cite_brnich and cite_brnich.strip().upper() == "TRUE":
        method_pmids.add(BRNICH_PMID)
    if thresholds_pmid and thresholds_pmid.strip():
        threshold_pmids.add(thresholds_pmid.strip())
    if oddspaths_pmid and oddspaths_pmid.strip():
        method_pmids.add(oddspaths_pmid.strip())
    if calculation_pmid and calculation_pmid.strip():
        calculation_pmids.add(calculation_pmid.strip())

    threshold_pubs = [
        PublicationIdentifierCreate(identifier=p, db_name="PubMed") for p in sorted(threshold_pmids) if p != "cvfg"
    ]
    method_pubs = [
        PublicationIdentifierCreate(identifier=p, db_name="PubMed") for p in sorted(method_pmids) if p != "cvfg"
    ]
    calculation_pubs = [
        PublicationIdentifierCreate(identifier=p, db_name="PubMed") for p in sorted(calculation_pmids) if p != "cvfg"
    ]
    return threshold_pubs, method_pubs, calculation_pubs


def build_ranges(row: Dict[str, str], infer_strengths: bool = True) -> Tuple[List[Any], bool]:
    ranges = []
    any_oddspaths = False
    for i in range(1, MAX_RANGES + 1):
        range_key = f"class_{i}_range"
        interval_text = row.get(range_key, "").strip()
        if not interval_text:
            click.echo(f"   Skipping empty interval in row: skipped class {i}", err=True)
            continue

        try:
            lower, upper, incl_lower, incl_upper = parse_interval(interval_text)
        except ValueError as e:
            click.echo(f"   Skipping invalid interval in row: {e}; skipped class {i}", err=True)
            continue

        strength_raw = row.get(f"class_{i}_strength", "").strip()
        if strength_raw not in [
            "BS3_STRONG",
            "BS3_MODERATE",
            "BS3_SUPPORTING",
            "INDETERMINATE",
            "PS3_VERY_STRONG",
            "PS3_STRONG",
            "PS3_MODERATE",
            "PS3_SUPPORTING",
            "",
        ]:
            click.echo(f"   Invalid strength '{strength_raw}' in row; inferring strength from oddspaths", err=True)
            strength_raw = ""

        classification = normalize_classification(row.get(f"class_{i}_functional_classification"), strength_raw)
        oddspaths_raw = row.get(f"class_{i}_odds_path", "").strip()
        oddspaths_ratio = None
        evidence_classification = None
        if oddspaths_raw:
            any_oddspaths = True

            try:
                oddspaths_ratio = float(oddspaths_raw)
            except ValueError:
                click.echo(f"   Skipping invalid odds_path '{oddspaths_raw}' in row; skipped class {i}", err=True)
                continue

            if not strength_raw and infer_strengths:
                criterion, strength = oddspaths_evidence_strength_equivalent(oddspaths_ratio)
            elif strength_raw:
                criterion = ACMGCriterion.PS3 if strength_raw.startswith("PS") else ACMGCriterion.BS3
                if strength_raw.endswith("VERY_STRONG"):
                    strength = StrengthOfEvidenceProvided.VERY_STRONG
                elif strength_raw.endswith("STRONG"):
                    strength = StrengthOfEvidenceProvided.STRONG
                elif strength_raw.endswith("MODERATE"):
                    strength = StrengthOfEvidenceProvided.MODERATE
                elif strength_raw.endswith("SUPPORTING"):
                    strength = StrengthOfEvidenceProvided.SUPPORTING
            else:
                criterion, strength = None, None

            if criterion and strength:
                evidence_classification = ACMGClassificationCreate(criterion=criterion, evidence_strength=strength)
            else:
                evidence_classification = None

        label = row.get(f"class_{i}_name", "").strip()
        ranges.append(
            FunctionalClassificationCreate(
                label=label,
                classification=classification,
                range=(lower, upper),
                inclusive_lower_bound=incl_lower if lower is not None else False,
                inclusive_upper_bound=incl_upper if upper is not None else False,
                acmg_classification=evidence_classification,
                oddspaths_ratio=oddspaths_ratio if oddspaths_ratio else None,
            )
        )
    return ranges, any_oddspaths


@click.command()
@with_database_session
@click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--delimiter", default=",", show_default=True, help="CSV delimiter")
@click.option("--overwrite", is_flag=True, default=False, help="Overwrite existing container for each score set")
@click.option(
    "--purge-publication-relationships", is_flag=True, default=False, help="Purge existing publication relationships"
)
def main(db: Session, csv_path: str, delimiter: str, overwrite: bool, purge_publication_relationships: bool):
    """Load calibration CSV into score set score_calibrations.

    Rows skipped if no URNs or no valid ranges. Only the targeted container key is replaced (unless --overwrite).
    """
    path = Path(csv_path)
    updated_sets = 0
    skipped_rows = 0
    errors = 0
    processed_rows = 0

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for row in reader:
            processed_rows += 1
            urn_cell = row.get("score_set_urn", "")
            if not urn_cell:
                skipped_rows += 1
                click.echo(f"No URN found in source CSV; skipping row {processed_rows}", err=True)
                continue

            urns = [u.strip() for u in urn_cell.split(",") if u.strip()]
            if not urns:
                skipped_rows += 1
                click.echo(f"No URN found in source CSV; skipping row {processed_rows}", err=True)
                continue

            click.echo(f"Processing row {processed_rows} for score set URNs: {', '.join(urns)}")

            threshold_pubs, method_pubs, calculation_pubs = build_publications(
                row.get("cite_brnich_method", ""),
                row.get("thresholds_pmid", ""),
                row.get("methods_pmid", ""),
                row.get("odds_path_pmid", ""),
            )

            ranges, any_oddspaths = build_ranges(row, infer_strengths=True)

            # baseline score only for brnich-style wrappers
            baseline_raw = row.get("baseline_score", "").strip()
            baseline_score = None
            if baseline_raw:
                try:
                    baseline_score = float(baseline_raw)
                except ValueError:
                    click.echo(
                        f"Invalid baseline_score '{baseline_raw}' ignored; row {processed_rows} will still be processed",
                        err=True,
                    )

            baseline_score_description_raw = row.get("baseline_score_notes", "").strip()
            calibration_notes_raw = row.get("calibration_notes_for_mavedb", "").strip()
            calibration_name_raw = row.get("calibration_name", "investigator_provided").strip().lower()
            calibration_is_investigator_provided = calibration_name_raw == "investigator_provided"
            calibration_name = NAME_ALIASES.get(calibration_name_raw, calibration_name_raw)
            baseline_score_description = baseline_score_description_raw if baseline_score_description_raw else None
            threshold_publications = threshold_pubs if threshold_pubs else []
            method_publications = method_pubs if method_pubs else []
            calculation_publications = calculation_pubs if calculation_pubs else []
            primary = row.get("primary", "").strip().upper() == "TRUE"
            calibration_notes = calibration_notes_raw if calibration_notes_raw else None

            try:
                created_score_calibration = ScoreCalibrationCreate(
                    title=calibration_name,
                    baseline_score=baseline_score,
                    baseline_score_description=baseline_score_description,
                    threshold_sources=threshold_publications,
                    method_sources=method_publications,
                    classification_sources=calculation_publications,
                    research_use_only=False,
                    functional_classifications=ranges,
                    notes=calibration_notes,
                )
            except Exception as e:  # broad to keep import running
                errors += 1
                click.echo(f"Validation error building container: {e}; skipping row {processed_rows}", err=True)
                continue

            for urn in urns:
                created_score_calibration.score_set_urn = urn
                score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
                if not score_set:
                    click.echo(f"Score set with URN {urn} not found; skipping row {processed_rows}", err=True)
                    errors += 1
                    continue

                existing_calibration_object = (
                    db.query(score_calibration.ScoreCalibration)
                    .filter(
                        score_calibration.ScoreCalibration.score_set_id == score_set.id,
                        score_calibration.ScoreCalibration.title == calibration_name,
                    )
                    .one_or_none()
                )
                if overwrite and existing_calibration_object:
                    replaced = True
                    db.delete(existing_calibration_object)
                else:
                    replaced = False

                # Never purge primary relationships.
                if purge_publication_relationships and score_set.publication_identifier_associations:
                    for assoc in score_set.publication_identifier_associations:
                        if {"identifier": assoc.publication.identifier, "db_name": assoc.publication.db_name} in [
                            p.model_dump()
                            for p in threshold_publications + method_publications + calculation_publications
                        ] and not assoc.primary:
                            db.delete(assoc)

                if not replaced and existing_calibration_object:
                    skipped_rows += 1
                    click.echo(
                        f"Calibration {existing_calibration_object.title} exists for {urn}; use --overwrite to replace; skipping row {processed_rows}",
                        err=True,
                    )
                    continue

                system_user = db.query(User).filter(User.id == 1).one()
                calibration_user = score_set.created_by if calibration_is_investigator_provided else system_user
                new_calibration_object = asyncio.run(
                    create_score_calibration_in_score_set(db, created_score_calibration, calibration_user)
                )
                new_calibration_object.primary = primary
                new_calibration_object.private = False

                db.add(new_calibration_object)
                db.flush()
                updated_sets += 1

    click.echo(
        f"Processed {processed_rows} rows; Updated {updated_sets} score sets; Skipped {skipped_rows} rows; Errors {errors}."
    )


if __name__ == "__main__":  # pragma: no cover
    main()
