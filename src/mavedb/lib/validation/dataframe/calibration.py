import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.validation.constants.general import (
    calibration_class_column_name,
    calibration_control_status_column_name,
    calibration_variant_column_name,
    hgvs_nt_column,
    hgvs_pro_column,
)
from mavedb.lib.validation.dataframe.column import validate_data_column, validate_variant_column
from mavedb.lib.validation.dataframe.dataframe import standardize_dataframe, validate_no_null_rows
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.view_models import score_calibration

STANDARD_CALIBRATION_COLUMNS = (
    calibration_variant_column_name,
    calibration_class_column_name,
    hgvs_nt_column,
    hgvs_pro_column,
)

STANDARD_CALIBRATION_CONTROL_COLUMNS = (
    calibration_variant_column_name,
    calibration_control_status_column_name,
    hgvs_nt_column,
    hgvs_pro_column,
)


def validate_and_standardize_calibration_classes_dataframe(
    db: Session,
    score_set: ScoreSet,
    calibration: score_calibration.ScoreCalibrationCreate | score_calibration.ScoreCalibrationModify,
    classes_df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """
    Validate and standardize a calibration classes dataframe for functional classification calibrations.

    This function performs comprehensive validation of a calibration classes dataframe, ensuring
    it meets the requirements for functional classification calibrations. It standardizes column
    names, validates data integrity, and checks that variants and classes are properly formatted.

    Args:
        db (Session): Database session for validation queries.
        score_set (ScoreSet): The score set associated with the calibration.
        calibration (ScoreCalibrationCreate | ScoreCalibrationModify): The calibration object
            containing configuration details. Must be class-based.
        classes_df (pd.DataFrame): The input dataframe containing calibration classes data.

    Returns:
        pd.DataFrame: The standardized and validated calibration classes dataframe.

    Raises:
        ValueError: If the calibration is not class-based.
        ValidationError: If the dataframe contains invalid data, unexpected columns,
            invalid variant URNs, or improperly formatted classes.

    Note:
        The function expects the dataframe to contain specific columns for variants and
        calibration classes, and performs strict validation on both column structure
        and data content.
    """
    if not calibration.class_based:
        raise ValidationError(
            "Calibration classes file can only be provided for functional classification calibrations."
        )

    standardized_classes_df = standardize_dataframe(classes_df, STANDARD_CALIBRATION_COLUMNS)
    validate_calibration_df_column_names(standardized_classes_df)
    validate_no_null_rows(standardized_classes_df)

    column_mapping = {c.lower(): c for c in standardized_classes_df.columns}
    index_column = choose_calibration_index_column(standardized_classes_df)

    # Drop rows where the calibration class column is NA
    standardized_classes_df = standardized_classes_df.dropna(
        subset=[column_mapping[calibration_class_column_name]]
    ).reset_index(drop=True)

    for c in column_mapping:
        if c in {calibration_variant_column_name, hgvs_nt_column, hgvs_pro_column}:
            validate_variant_column(standardized_classes_df[c], column_mapping[c] == index_column)
        elif c == calibration_class_column_name:
            validate_data_column(standardized_classes_df[c], force_numeric=False)
            validate_calibration_classes(calibration, standardized_classes_df[c])

        if column_mapping[c] == index_column:
            validate_index_existence_in_score_set(
                db, score_set, standardized_classes_df[column_mapping[c]], column_mapping[c]
            )

    return standardized_classes_df, index_column


def validate_calibration_df_column_names(df: pd.DataFrame) -> None:
    """
    Validate the column names of a calibration DataFrame.

    This function performs comprehensive validation of DataFrame column names to ensure
    they meet the required format and structure for calibration data processing.

    Args:
        df (pd.DataFrame): The DataFrame whose columns need to be validated.

    Raises:
        ValidationError: If any of the following validation checks fail:
            - Column names are not strings
            - Column names are empty or contain only whitespace
            - Required calibration variant column is missing
            - Required calibration class column is missing
            - DataFrame contains unexpected columns (must match STANDARD_CALIBRATION_COLUMNS exactly)

    Returns:
        None: This function performs validation only and returns nothing on success.

    Note:
        Column name comparison is case-insensitive. The function converts all column
        names to lowercase before performing validation checks.
    """
    if any(type(c) is not str for c in df.columns):
        raise ValidationError("column names must be strings")

    if any(c.isspace() for c in df.columns) or any(len(c) == 0 for c in df.columns):
        raise ValidationError("column names cannot be empty or whitespace")

    if len(df.columns) != len(set(c.lower() for c in df.columns)):
        raise ValidationError("duplicate column names are not allowed (case-insensitive)")

    columns = [c.lower() for c in df.columns]

    if calibration_class_column_name not in columns:
        raise ValidationError(f"missing required column: '{calibration_class_column_name}'")

    if set(columns).isdisjoint({hgvs_nt_column, hgvs_pro_column, calibration_variant_column_name}):
        raise ValidationError(
            f"at least one of {', '.join({hgvs_nt_column, hgvs_pro_column, calibration_variant_column_name})} must be present"
        )


def validate_index_existence_in_score_set(
    db: Session, score_set: ScoreSet, index_column: pd.Series, index_column_name: str
) -> None:
    """
    Validate that all provided resources in the index column exist in the given score set.

    Args:
        db (Session): Database session for querying variants.
        score_set (ScoreSet): The score set to validate variants against.
        variant_urns (pd.Series): Series of variant URNs to validate.

    Raises:
        ValidationError: If any variant URNs do not exist in the score set.

    Returns:
        None: Function returns nothing if validation passes.
    """
    if index_column_name.lower() == calibration_variant_column_name:
        existing_resources = set(
            db.scalars(
                select(Variant.urn).where(Variant.score_set_id == score_set.id, Variant.urn.in_(index_column.tolist()))
            ).all()
        )
    elif index_column_name.lower() == hgvs_nt_column:
        existing_resources = set(
            db.scalars(
                select(Variant.hgvs_nt).where(
                    Variant.score_set_id == score_set.id, Variant.hgvs_nt.in_(index_column.tolist())
                )
            ).all()
        )
    elif index_column_name.lower() == hgvs_pro_column:
        existing_resources = set(
            db.scalars(
                select(Variant.hgvs_pro).where(
                    Variant.score_set_id == score_set.id, Variant.hgvs_pro.in_(index_column.tolist())
                )
            ).all()
        )

    missing_resources = set(index_column.tolist()) - existing_resources
    if missing_resources:
        raise ValidationError(
            f"The following resources do not exist in the score set: {', '.join(sorted(missing_resources))}"
        )


def choose_calibration_index_column(df: pd.DataFrame) -> str:
    """
    Choose the appropriate index column for a calibration DataFrame.

    This function selects the index column based on the presence of specific columns
    in the DataFrame. It prioritizes the calibration variant column, followed by
    HGVS notation columns.

    Args:
        df (pd.DataFrame): The DataFrame from which to choose the index column.

    Returns:
        str: The name of the chosen index column.

    Raises:
        ValidationError: If no valid index column is found in the DataFrame.
    """
    column_mapping = {c.lower(): c for c in df.columns if not df[c].isna().all()}

    if calibration_variant_column_name in column_mapping:
        return column_mapping[calibration_variant_column_name]
    elif hgvs_nt_column in column_mapping:
        return column_mapping[hgvs_nt_column]
    elif hgvs_pro_column in column_mapping:
        return column_mapping[hgvs_pro_column]
    else:
        raise ValidationError("failed to find valid calibration index column")


def validate_calibration_classes(
    calibration: score_calibration.ScoreCalibrationCreate | score_calibration.ScoreCalibrationModify, classes: pd.Series
) -> None:
    """
    Validate that the functional classifications in a calibration match the provided classes.

    This function ensures that:
    1. The calibration has functional classifications defined
    2. All classes in the provided series are defined in the calibration
    3. All classes defined in the calibration are present in the provided series

    Args:
        calibration: A ScoreCalibrationCreate or ScoreCalibrationModify object containing
                    functional classifications to validate against.
        classes: A pandas Series containing class labels to validate.

    Raises:
        ValueError: If the calibration does not have functional classifications defined.
        ValidationError: If there are classes in the series that are not defined in the
                        calibration, or if there are classes defined in the calibration
                        that are missing from the series.
    """
    if not calibration.functional_classifications:
        raise ValidationError("Calibration must have functional classifications defined for class validation.")

    defined_classes = {c.class_ for c in calibration.functional_classifications}
    provided_classes = set(classes.tolist())

    undefined_classes = provided_classes - defined_classes
    if undefined_classes:
        raise ValidationError(
            f"The following classes are not defined in the calibration: {', '.join(sorted(undefined_classes))}"
        )

    unprovided_classes = defined_classes - provided_classes
    if unprovided_classes:
        raise ValidationError("Some defined classes in the calibration are missing from the classes file.")


def validate_and_standardize_calibration_controls_dataframe(
    db: Session,
    score_set: ScoreSet,
    controls_df: pd.DataFrame,
) -> list[score_calibration.CalibrationControlCreate]:
    """Validate a calibration controls CSV and convert it to ``CalibrationControlCreate`` rows.

    Mirrors :func:`validate_and_standardize_calibration_classes_dataframe`. The CSV must carry a
    variant identifier column (``variant_urn``, ``hgvs_nt``, or ``hgvs_pro``) and a
    ``clinical_status`` column. Every variant must belong to the score set, and every status must be
    a ``CalibrationControlStatus`` value (case-insensitive). The chosen variant column is resolved to
    MaveDB URNs so the returned controls carry ``variant_urn``, matching the inline-JSON contract.

    Args:
        db: Database session for validation and URN resolution.
        score_set: The score set the calibration belongs to.
        controls_df: The parsed controls CSV.

    Returns:
        One ``CalibrationControlCreate`` per row.

    Raises:
        ValidationError: On unexpected/missing columns, invalid variants, or invalid status values.
    """
    standardized_df = standardize_dataframe(controls_df, STANDARD_CALIBRATION_CONTROL_COLUMNS)
    validate_calibration_control_df_column_names(standardized_df)
    validate_no_null_rows(standardized_df)

    column_mapping = {c.lower(): c for c in standardized_df.columns}
    index_column = choose_calibration_index_column(standardized_df)

    # Drop rows without a clinical status.
    standardized_df = standardized_df.dropna(
        subset=[column_mapping[calibration_control_status_column_name]]
    ).reset_index(drop=True)

    for c in column_mapping:
        if c in {calibration_variant_column_name, hgvs_nt_column, hgvs_pro_column}:
            validate_variant_column(standardized_df[c], column_mapping[c] == index_column)
        elif c == calibration_control_status_column_name:
            validate_data_column(standardized_df[c], force_numeric=False)
            validate_calibration_control_statuses(standardized_df[column_mapping[c]])

        if column_mapping[c] == index_column:
            validate_index_existence_in_score_set(db, score_set, standardized_df[column_mapping[c]], column_mapping[c])

    return resolve_calibration_controls(db, score_set, standardized_df, index_column)


def validate_calibration_control_df_column_names(df: pd.DataFrame) -> None:
    """Validate the column names of a calibration controls DataFrame.

    Requires a ``clinical_status`` column and at least one variant identifier column, and rejects
    unexpected columns. Comparison is case-insensitive. Mirrors
    :func:`validate_calibration_df_column_names`.
    """
    if any(type(c) is not str for c in df.columns):
        raise ValidationError("column names must be strings")

    if any(c.isspace() for c in df.columns) or any(len(c) == 0 for c in df.columns):
        raise ValidationError("column names cannot be empty or whitespace")

    if len(df.columns) != len(set(c.lower() for c in df.columns)):
        raise ValidationError("duplicate column names are not allowed (case-insensitive)")

    columns = [c.lower() for c in df.columns]

    if calibration_control_status_column_name not in columns:
        raise ValidationError(f"missing required column: '{calibration_control_status_column_name}'")

    if set(columns).isdisjoint({hgvs_nt_column, hgvs_pro_column, calibration_variant_column_name}):
        raise ValidationError(
            f"at least one of {', '.join({hgvs_nt_column, hgvs_pro_column, calibration_variant_column_name})} must be present"
        )


def validate_calibration_control_statuses(statuses: pd.Series) -> None:
    """Ensure every ``clinical_status`` value maps to a ``CalibrationControlStatus`` (case-insensitive)."""
    valid_statuses = {status.value for status in CalibrationControlStatus}
    invalid_statuses = {
        str(status) for status in statuses.tolist() if str(status).strip().lower() not in valid_statuses
    }
    if invalid_statuses:
        raise ValidationError(
            f"Invalid clinical status value(s): {', '.join(sorted(invalid_statuses))}. "
            f"Allowed values are: {', '.join(sorted(valid_statuses))}."
        )


def resolve_calibration_controls(
    db: Session,
    score_set: ScoreSet,
    df: pd.DataFrame,
    index_column: str,
) -> list[score_calibration.CalibrationControlCreate]:
    """Convert a validated controls DataFrame to ``CalibrationControlCreate`` rows, resolving URNs.

    For a ``variant_urn`` index the URN is taken directly. For an HGVS index the values are mapped to
    their variant URNs within the score set in a single query. A control is a single variant, so an
    HGVS that matches more than one variant is ambiguous and rejected — whether a given HGVS column
    collides depends on the score set's own index (``hgvs_pro`` is unique in a protein-indexed set but
    not in a nucleotide-indexed one), so this is checked against the data rather than assumed.
    """
    column_mapping = {c.lower(): c for c in df.columns}
    status_column = column_mapping[calibration_control_status_column_name]
    index_values = df[index_column].tolist()

    if index_column.lower() == calibration_variant_column_name:
        urn_by_index_value = {value: value for value in index_values}
    else:
        hgvs_attribute = Variant.hgvs_nt if index_column.lower() == hgvs_nt_column else Variant.hgvs_pro
        urns_by_hgvs: dict[str, list[str]] = {}
        for urn, hgvs in db.execute(
            select(Variant.urn, hgvs_attribute).where(
                Variant.score_set_id == score_set.id, hgvs_attribute.in_(index_values)
            )
        ).all():
            urns_by_hgvs.setdefault(hgvs, []).append(urn)

        ambiguous_identifiers = sorted(hgvs for hgvs, urns in urns_by_hgvs.items() if len(urns) > 1)
        if ambiguous_identifiers:
            raise ValidationError(
                "These variant identifiers match more than one variant in the score set; "
                f"use the variant URN instead: {', '.join(ambiguous_identifiers)}."
            )

        urn_by_index_value = {hgvs: urns[0] for hgvs, urns in urns_by_hgvs.items()}

    return [
        score_calibration.CalibrationControlCreate(
            variant_urn=urn_by_index_value[row[index_column]],
            clinical_status=CalibrationControlStatus(str(row[status_column]).strip().lower()),
        )
        for _, row in df.iterrows()
    ]
