import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.validation.constants.general import (
    calibration_class_column_name,
    calibration_variant_column_name,
)
from mavedb.lib.validation.dataframe.column import validate_data_column, validate_variant_column
from mavedb.lib.validation.dataframe.dataframe import standardize_dataframe, validate_no_null_rows
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.view_models import score_calibration

STANDARD_CALIBRATION_COLUMNS = (calibration_variant_column_name, calibration_class_column_name)


def validate_and_standardize_calibration_classes_dataframe(
    db: Session,
    score_set: ScoreSet,
    calibration: score_calibration.ScoreCalibrationCreate | score_calibration.ScoreCalibrationModify,
    classes_df: pd.DataFrame,
) -> pd.DataFrame:
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
        raise ValueError("Calibration classes file can only be provided for functional classification calibrations.")

    standardized_classes_df = standardize_dataframe(classes_df, STANDARD_CALIBRATION_COLUMNS)
    validate_calibration_df_column_names(standardized_classes_df)
    validate_no_null_rows(standardized_classes_df)

    column_mapping = {c.lower(): c for c in standardized_classes_df.columns}
    index_column = column_mapping[calibration_variant_column_name]

    for c in column_mapping:
        if c == calibration_variant_column_name:
            validate_variant_column(standardized_classes_df[c], column_mapping[c] == index_column)
            validate_calibration_variant_urns(db, score_set, standardized_classes_df[c])
        elif c == calibration_class_column_name:
            validate_data_column(standardized_classes_df[c], force_numeric=False)
            validate_calibration_classes(calibration, standardized_classes_df[c])

        # handle unexpected columns. These should have already been caught by
        # validate_calibration_df_column_names, but we include this for completeness.
        else:  # pragma: no cover
            raise ValidationError(f"unexpected column in calibration classes file: '{c}'")

    return standardized_classes_df


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

    if calibration_variant_column_name not in columns:
        raise ValidationError(f"missing required column: '{calibration_variant_column_name}'")

    if calibration_class_column_name not in columns:
        raise ValidationError(f"missing required column: '{calibration_class_column_name}'")

    if set(STANDARD_CALIBRATION_COLUMNS) != set(columns):
        raise ValidationError(
            f"unexpected column(s) in calibration classes file: {', '.join(sorted(set(columns) - set(STANDARD_CALIBRATION_COLUMNS)))}"
        )


def validate_calibration_variant_urns(db: Session, score_set: ScoreSet, variant_urns: pd.Series) -> None:
    """
    Validate that all provided variant URNs exist in the given score set.

    Args:
        db (Session): Database session for querying variants.
        score_set (ScoreSet): The score set to validate variants against.
        variant_urns (pd.Series): Series of variant URNs to validate.

    Raises:
        ValidationError: If any variant URNs do not exist in the score set.

    Returns:
        None: Function returns nothing if validation passes.
    """
    existing_variant_urns = set(
        db.scalars(
            select(Variant.urn).where(Variant.score_set_id == score_set.id, Variant.urn.in_(variant_urns.tolist()))
        ).all()
    )

    missing_variant_urns = set(variant_urns.tolist()) - existing_variant_urns
    if missing_variant_urns:
        raise ValidationError(
            f"The following variant URNs do not exist in the score set: {', '.join(sorted(missing_variant_urns))}"
        )


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
        raise ValueError("Calibration must have functional classifications defined for class validation.")

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
