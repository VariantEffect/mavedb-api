import pandas as pd
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from mavehgvs.variant import Variant
from mavehgvs.exceptions import MaveHgvsParseError
import numpy as np

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_splice_column,
    hgvs_pro_column,
    required_score_column
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.variant import validate_hgvs_string
from mavedb.lib.validation.utilities import convert_hgvs_nt_to_hgvs_pro
from mavedb.lib.validation.target import validate_target_sequence
from fqfa.util.infer import infer_sequence_type
from fqfa.util.nucleotide import (
    reverse_complement,
    convert_dna_to_rna,
    convert_rna_to_dna,
)
from fqfa.util.translate import translate_dna
# handle with pandas all null strings
# provide a csv or a pandas dataframe
# take dataframe, output as csv to temp directory, use standard library


class DataframeValidationError(ValueError):
    pass


STANDARD_COLUMNS = (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, required_score_column)


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


def sort_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sort the columns of the given dataframe according to the expected ordering in MaveDB.

    MaveDB expects that dataframes have columns in the following order (note some columns are optional):
    * hgvs_nt
    * hgvs_splice
    * hgvs_pro
    * score
    * other

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe with columns to sort

    Returns
    -------
    pandas.DataFrame
        The dataframe with the same data but sorted columns
    """

    def column_sort_function(value, columns):
        if value.lower() in STANDARD_COLUMNS:
            return STANDARD_COLUMNS.index(value.lower())
        else:
            return columns.index(value) + len(STANDARD_COLUMNS)

    old_columns = list(df.columns)
    new_columns = sorted(old_columns, key=lambda v: column_sort_function(v, old_columns))

    return df[new_columns]


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize a dataframe by sorting the columns and changing the standard column names to lowercase.

    The standard column names are:
    * hgvs_nt
    * hgvs_splice
    * hgvs_pro
    * score

    Case for other columns is preserved.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to standardize

    Returns
    -------
    pandas.DataFrame
        The standardized dataframe
    """
    new_columns = [x.lower() if x.lower() in STANDARD_COLUMNS else x for x in df.columns]
    df.columns = new_columns

    return sort_dataframe_columns(df)


def validate_dataframes(target_seq: str, scores, counts=None) -> None:
    """Validates scores and counts dataframes for MaveDB upload.

    This function performs comprehensive validation by calling other validators.

    Parameters
    __________
    scores : pandas.DataFrame
        The scores data as a pandas dataframe.
    counts : pandas.DataFrame
        The counts data as a pandas dataframe.

    Raises
    ______
    ValidationError
        If any of the validation fails.
    """
    #validate_no_null_columns_or_rows(scores)
    scores = validate_column_names(scores, kind="scores")
    validate_values_by_column(scores, target_seq)
    if counts is not None:
        #validate_no_null_columns_or_rows(counts)
        counts = validate_column_names(counts, kind="counts")
        validate_values_by_column(counts, target_seq)
        validate_dataframes_define_same_variants(scores, counts)


def validate_no_null_data_columns(df: pd.DataFrame) -> None:
    """Check that there are no null data columns (non-HGVS) in the dataframe.

    Note that a null column may still have a valid column name.

    Parameters
    __________
    df : pandas.DataFrame
        The scores or counts dataframe being validated

    Raises
    ______
    DataframeValidationError
        If there are null data columns in the dataframe
    """
    null_columns = list()
    for cname in df.columns:
        if cname in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            continue
        else:
            if infer_column_type(df[cname]) == "empty":
                null_columns.append(cname)

    if len(null_columns) > 0:
        null_column_string = ", ".join(f"'{c}'" for c in null_columns)
        raise DataframeValidationError(f"data columns contains no data: {null_column_string}")


def validate_no_null_rows(df: pd.DataFrame) -> None:
    """Check that there are no fully null rows in the dataframe.

    Parameters
    __________
    df : pandas.DataFrame
        The scores or counts dataframe being validated

    Raises
    ______
    DataframeValidationError
        If there are null rows in the dataframe
    """
    null_rows = df.apply(lambda row: np.all(row.isna()), axis=1)
    if sum(null_rows) > 0:
        raise DataframeValidationError(f"found {sum(null_rows)} null rows in the data frame")


def validate_column_names(df: pd.DataFrame, kind: str) -> None:
    """Validate the column names in a dataframe.

    This function validates the column names in the given dataframe.
    It can be run for either a "scores" dataframe or a "counts" dataframe.
    A "scores" dataframe must have a column named 'score' and a "counts" dataframe cannot have a column named 'score'.

    The function also checks for a valid combination of columns that define variants.

    Basic checks are performed to make sure that a column name is not empty, null, or whitespace,
    as well as making sure there are no duplicate column names.

    Parameters
    ----------
    df : pandas.DataFrame
        The scores or counts dataframe to be validated

    kind : str
        Either "counts" or "scores" depending on the kind of dataframe being validated

    Raises
    ------
    DataframeValidationError
        If the column names are not valid
    """
    if any(type(c) != str for c in df.columns):
        raise DataframeValidationError("column names must be strings")

    if any(c.isspace() for c in df.columns) or any(len(c) == 0 for c in df.columns):
        raise DataframeValidationError("column names cannot be empty or whitespace")

    columns = [c.lower() for c in df.columns]

    if kind == "scores":
        if required_score_column not in columns:
            raise DataframeValidationError(f"score dataframe must have a '{required_score_column}' column")
    elif kind == "counts":
        if required_score_column in columns:
            raise DataframeValidationError(f"counts dataframe must not have a '{required_score_column}' column")
    else:
        raise ValueError("kind only accepts scores and counts")

    if hgvs_splice_column in columns:
        if hgvs_nt_column not in columns or hgvs_pro_column not in columns:
            raise DataframeValidationError(f"dataframes with '{hgvs_splice_column}' must also define '{hgvs_nt_column}' and '{hgvs_pro_column}'")

    if len(columns) != len(set(columns)):
        raise DataframeValidationError("duplicate column names are not allowed (this check is case insensitive)")

    if set(columns).isdisjoint({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column}):
        raise DataframeValidationError("dataframe does not define any variant columns")

    if set(columns).issubset({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column}):
        raise DataframeValidationError("dataframe does not define any data columns")


def validate_hgvs_columns(df: pd.DataFrame, targetseq: str, targetseq_type: str = "infer") -> None:
    """Validate the contents of the HGVS variant columns.

    This function assumes that the validate_column_names validator has already been passed.

    Parameters
    ----------
    df : pandas.DataFrame
        A scores or counts dataframe
    targetseq : str
        The target sequence to validate the variants against
    targetseq_type : str
        The type of target sequence, which can be one of "dna", "protein", or "infer"

    Raises
    ------
    ValueError
        If the target sequence is not DNA or protein
    ValueError
        If a dataframe with no HGVS variant columns is passed
    DataframeValidationError
        If any variant fails validation
    """
    if targetseq_type == "infer":
        targetseq_type = infer_sequence_type(targetseq)

    if targetseq_type not in ("dna", "protein"):
        raise ValueError("invalid target sequence type")

    # map the columns so we can ignore case and track what's present
    hgvs_columns = dict()
    for c in df.columns:
        if c.lower() == hgvs_nt_column:
            if not df[c].isna().all():
                hgvs_columns[hgvs_nt_column] = c
        elif c.lower() == hgvs_splice_column:
            if not df[c].isna().all():
                hgvs_columns[hgvs_splice_column] = c
        elif c.lower() == hgvs_pro_column:
            if not df[c].isna().all():
                hgvs_columns[hgvs_pro_column] = c

    # determine the index column
    if hgvs_splice_column in hgvs_columns:
        index_column = hgvs_columns[hgvs_nt_column]
    elif hgvs_nt_column in hgvs_columns:
        index_column = hgvs_columns[hgvs_nt_column]
    elif hgvs_pro_column in hgvs_columns:
        index_column = hgvs_columns[hgvs_pro_column]
    else:
        raise ValueError("cannot validate dataframe with no hgvs columns")

    # make sure the hgvs columns are string columns
    for k in hgvs_columns:
        if infer_column_type(df[hgvs_columns[k]]) != "string":
            raise DataframeValidationError(f"variant column '{k}' cannot contain numeric data")

    # test that the index column is complete and unique
    if df[index_column].isna().any():
        raise DataframeValidationError(f"primary variant column '{index_column}' cannot contain null values")
    if not df[index_column].is_unique:
        raise DataframeValidationError(f"primary variant column '{index_column}' must contain unique values")

    # test prefix consistency for protein (must be "p.")
    if hgvs_pro_column in hgvs_columns:
        strings = df[hgvs_columns[hgvs_pro_column]].dropna()
        if not all(s.startswith("p.") for s in strings):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_pro_column]}' has invalid variant prefixes")

    # test prefix consistency for splice (must be "c.")
    if hgvs_splice_column in hgvs_columns:
        strings = df[hgvs_columns[hgvs_splice_column]].dropna()
        if not all(s.startswith("c.") for s in strings):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_splice_column]}' has invalid variant prefixes")

    # test prefix consistency for nt (must be "cngmor." and consistent)
    if hgvs_nt_column in hgvs_columns:
        prefixes = [f"{a}." for a in "cngmor"]
        strings = df[hgvs_columns[hgvs_nt_column]].dropna()
        if len(set(s[:2] for s in strings)) > 1:
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' has inconsistent variant prefixes")
        elif not all(s[:2] in prefixes for s in strings):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' has invalid variant prefixes")

    # test agreement of prefixes across columns
    if hgvs_splice_column in hgvs_columns:
        if df[hgvs_columns[hgvs_nt_column]][0][:2] not in ("g.", "m.", "o."):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' must use valid genomic prefix when '{hgvs_columns[hgvs_splice_column]}' is present")
    elif hgvs_pro_column in hgvs_columns and hgvs_nt_column in hgvs_columns:
        if df[hgvs_columns[hgvs_nt_column]][0][:2] != "c.":
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' must use 'c.' prefix when '{hgvs_columns[hgvs_pro_column]}' is present and '{hgvs_columns[hgvs_splice_column]}' is absent")
    elif hgvs_nt_column in hgvs_columns:  # just hgvs_nt
        if df[hgvs_columns[hgvs_nt_column]][0][:2] not in ("n.", "r."):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' does not have a valid prefix for nucleotide-only variants")

    # validate hgvs strings for internal consistency
    invalid_variants = list()
    if hgvs_splice_column in hgvs_columns:
        for i, s in df[hgvs_columns[hgvs_splice_column]].items():
            if s is not None:
                try:
                    Variant(s)  # splice variants are not validated against the target sequence
                except MaveHgvsParseError:
                    invalid_variants.append(f"invalid variant string '{s}' at row {i}")

    if hgvs_nt_column in hgvs_columns:
        if df[hgvs_columns[hgvs_nt_column]][0].startswith("r"):
            targetseq = convert_dna_to_rna(targetseq).lower()
        for i, s in df[hgvs_columns[hgvs_nt_column]].items():
            if s is not None:
                try:
                    Variant(s, targetseq=targetseq)
                except MaveHgvsParseError:
                    try:
                        Variant(s)
                    except MaveHgvsParseError:
                        invalid_variants.append(f"invalid variant string '{s}' at row {i}")
                    else:
                        invalid_variants.append(f"target sequence mismatch for '{s}' at row {i}")

    if hgvs_pro_column in hgvs_columns:
        if targetseq_type == "dna":
            targetseq = translate_dna(targetseq)[0]
        for i, s in df[hgvs_columns[hgvs_pro_column]].items():
            if s is not None:
                try:
                    Variant(s, targetseq=targetseq)
                except MaveHgvsParseError:
                    try:
                        Variant(s)
                    except MaveHgvsParseError:
                        invalid_variants.append(f"invalid variant string '{s}' at row {i}")
                    else:
                        invalid_variants.append(f"target sequence mismatch for '{s}' at row {i}")

    if len(invalid_variants) > 0:
        raise DataframeValidationError(f"encountered {len(invalid_variants)} invalid variant strings: {(', '.join(invalid_variants))}")

    # validate hgvs strings for consistency across columns
    # TODO


def validate_data_columns(df: pd.DataFrame) -> None:
    """Validate the contents of the data columns.

    Parameters
    ----------
    df : pandas.DataFrame
        A scores or counts dataframe

    Raises
    ------
    DataframeValidationError
        If the data fails validation
    """
    if infer_column_type(df[required_score_column]) != "numeric":
        raise DataframeValidationError(f"column '{required_score_column}' must contain numeric values")
    # TODO extra columns


def validate_dataframes_define_same_variants(scores, counts):
    # TODO update
    """
    Checks if two `pd.DataFrame` objects parsed from uploaded files
    define the same variants.

    Parameters
    ----------
    scores : pandas.DataFrame
        Scores dataframe parsed from an uploaded scores file.
    counts : pandas.DataFrame
        Scores dataframe parsed from an uploaded counts file.

    Raises
    ______
    ValidationError
        If score and counts files do not define the same variants.
    """
    try:
        assert_array_equal(
            scores[hgvs_nt_column].sort_values().values,
            counts[hgvs_nt_column].sort_values().values,
        )
        assert_array_equal(
            scores[hgvs_splice_column].sort_values().values,
            counts[hgvs_splice_column].sort_values().values,
        )
        assert_array_equal(
            scores[hgvs_pro_column].sort_values().values,
            counts[hgvs_pro_column].sort_values().values,
        )
    except AssertionError:
        raise ValidationError(
            "Your score and counts files do not define the same variants. "
            "Check that the hgvs columns in both files match."
        )
