from typing import Optional

import numpy as np
import pandas as pd
from fqfa.util.translate import translate_dna

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.target_sequence import TargetSequence


def infer_column_type(col: pd.Series) -> str:
    """Infer whether the given column contains string or numeric data.

    The function returns "string" for string columns or "numeric" for numeric columns.
    If there is a mixture of types it returns "mixed".
    If every value in the column is `None` or NA it returns "empty".

    Parameters
    ----------
    col : pandas.Series
        The column to inspect

    Returns
    -------
    str
        One of "string", "numeric", "mixed", or "empty"
    """
    if col.isna().all():
        return "empty"
    else:
        col_numeric = pd.to_numeric(col, errors="coerce")
        if col_numeric.isna().all():  # nothing converted to a number
            return "string"
        elif np.all(col.isna() == col_numeric.isna()):  # all non-NA values converted
            return "numeric"
        else:  # some values converted but not all
            return "mixed"


def validate_variant_formatting(column: pd.Series, prefixes: list[str], targets: list[str], fully_qualified: bool):
    """
    Validate the formatting of HGVS variants present in the passed column against
    lists of prefixes and targets

    Parameters
    ----------
    column : pd.Series
        A pandas column containing HGVS variants
    prefixes : list[str]
        A list of prefixes we can expect to occur within the passed column
    targets : list[str]
        A list of targets we can expect to occur within the passed column

    Returns
    -------
    None

    Raises
    ------
    ValidationError
        If any of the variants in the column are not fully qualified with respect to multiple possible targets
    ValidationError
        If the column contains multiple prefixes or the wrong prefix for that column name
    ValidationError
        If the column contains target accessions not present in the list of possible targets
    """
    variants = [variant for s in column.dropna() for variant in s.split(" ")]

    # if there is more than one target, we expect variants to be fully qualified
    if fully_qualified:
        invalid_fully_qualified = [f"{len(str(v).split(':'))} invalid fully qualified found from row {idx}"
                                   for idx, v in enumerate(variants) if len(str(v).split(":")) != 2]
        if invalid_fully_qualified:
            raise ValidationError(
                f"variant column '{column.name}' has {len(invalid_fully_qualified)} unqualified variants.",
                triggers=invalid_fully_qualified
            )

        inconsistent_prefixes = [f"row {idx}: '{v}' uses inconsistent prefix '{str(v).split(':')[1][:2]}'"
                                 for idx, v in enumerate(variants)
                                 if len(set(str(v).split(":")[1][:2] for v in variants)) > 1]
        if inconsistent_prefixes:
            raise ValidationError(
                f"variant column '{column.name}' has {len(inconsistent_prefixes)} inconsistent variant prefixes.",
                triggers=inconsistent_prefixes
            )

        invalid_prefixes = [f"row {idx}: '{v}' uses invalid prefix '{str(v).split(':')[1][:2]}'"
                            for idx, v in enumerate(variants) if str(v).split(":")[1][:2] not in prefixes]
        if invalid_prefixes:
            raise ValidationError(
                f"variant column '{column.name}' has {len(invalid_prefixes)} invalid variant prefixes.",
                triggers=invalid_prefixes
            )

        invalid_accessions = [f"accession identifier {str(v).split(':')[0]} from row {idx}, variant {v} not found"
                              for idx, v in enumerate(variants) if str(v).split(":")[0] not in targets]
        if invalid_accessions:
            raise ValidationError(
                f"variant column '{column.name}' has invalid accession identifiers; "
                f"{len(invalid_accessions)} accession identifiers present in the score file were not added as targets.",
                triggers=invalid_accessions
            )

    else:
        if len(set(v[:2] for v in variants)) > 1:
            raise ValidationError(f"variant column '{column.name}' has inconsistent variant prefixes")
        if not all(v[:2] in prefixes for v in variants):
            raise ValidationError(f"variant column '{column.name}' has invalid variant prefixes")


def generate_variant_prefixes(column: pd.Series):
    """
    Generate variant prefixes for the provided column

    Parameters
    ----------
    column : pd.Series
        The pandas column from which to generate variant prefixes

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the provided pandas column has an unrecognized variant column name
    """
    if str(column.name).lower() == hgvs_nt_column:
        return [f"{a}." for a in "cngmo"]
    if str(column.name).lower() == hgvs_splice_column:
        return [f"{a}." for a in "cn"]
    if str(column.name).lower() == hgvs_pro_column:
        return ["p."]

    raise ValueError(f"unrecognized hgvs column name '{column.name}'")


def validate_variant_column(column: pd.Series, is_index: bool):
    """
    Validate critical column properties of an HGVS variant column, with special
    attention to certain properties expected on index columns

    Parameters
    ----------
    column : pd.Series
        The pandas column containing HGVS variant information
    id_index : bool
        Whether the provided column is the index column

    Returns
    -------
    None

    Raises
    ------
    ValidationError
        If an index column contains missing or non-unique values
    ValidationError
        If a column contains any numeric data
    """
    if infer_column_type(column) not in ("string", "empty"):
        raise ValidationError(f"variant column '{column.name}' cannot contain numeric data")
    if column.isna().any() and is_index:
        raise ValidationError(f"primary variant column '{column.name}' cannot contain null values")
    if not column.is_unique and is_index:
        raise ValidationError(f"primary variant column '{column.name}' must contain unique values")


def validate_data_column(column: pd.Series, force_numeric: bool = False) -> None:
    """
    Validate the contents of a data column.

    Parameters
    ----------
    column : pandas.Series
        A data column from a dataframe
    force_numeric : bool
        Force the data to be numeric, used for score column and count data

    Returns
    -------
    None

    Raises
    ------
    ValidationError
        If the data is all null
    ValidationError
        If the data is of mixed numeric and string types
    ValidationError
        If the data is not numeric and force_numeric is True

    """
    column_type = infer_column_type(column)
    if column_type == "empty":
        raise ValidationError(f"data column '{column.name}' contains no data")
    elif column_type == "mixed":
        raise ValidationError(f"data column '{column.name}' has mixed string and numeric types")
    elif force_numeric and column_type != "numeric":
        raise ValidationError(f"data column '{column.name}' must contain only numeric data")


def validate_hgvs_column_properties(column: pd.Series, observed_sequence_types: list[str]) -> None:
    """
    Validates the properties of an HGVS column in a DataFrame.

    Parameters
    ----------
    column : pd.Series
        The column to validate.
    observed_sequence_types : list[str]
        A list of observed sequence types.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the column name is 'hgvs_nt' and 'dna' is not in the observed sequence types.
    ValueError
        If the column name is not recognized as either 'hgvs_splice' or 'hgvs_pro'.
    """
    if str(column.name).lower() == hgvs_nt_column:
        if "dna" not in observed_sequence_types:
            raise ValueError(
                f"invalid target sequence type(s) for '{column.name}'. At least one target should be of type `dna`. Observed types: {observed_sequence_types}"
            )
    elif str(column.name).lower() != hgvs_splice_column and str(column.name).lower() != hgvs_pro_column:
        raise ValueError(f"unrecognized hgvs column name '{column.name}'")


def construct_target_sequence_mappings(
    column: pd.Series, targets: dict[str, TargetSequence]
) -> dict[str, Optional[str]]:
    """
    Constructs a mapping of target sequences based on the provided column and targets. Translates protein sequences
    to DNA sequences if needed for passed protein columns. Don't validate splice columns against provided sequences.

    Parameters
    ----------
    column : pd.Series
        The pandas Series representing the column to be validated.
    targets : dict[str, TargetSequence]
        A dictionary where keys are target names and values are TargetSequence objects.

    Returns
    -------
    dict[str, Union[str, pd.Series]]: A dictionary where keys are target names and values are either the target sequence,
                                      the translated target sequence, or None depending on the column type.
    """
    if str(column.name).lower() not in (hgvs_nt_column, hgvs_pro_column, hgvs_splice_column):
        raise ValueError(f"unrecognized hgvs column name '{column.name}'")

    if str(column.name).lower() == hgvs_splice_column:
        return {name: None for name in targets.keys()}

    return {
        name: translate_dna(target.sequence)[0]
        if (
            str(column.name).lower() == hgvs_pro_column
            and target.sequence_type == "dna"
            and target.sequence is not None
        )
        else target.sequence
        for name, target in targets.items()
    }
