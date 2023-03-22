import pandas as pd
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from mavehgvs.variant import Variant
from mavehgvs.exceptions import MaveHgvsParseError
import numpy as np
from typing import Optional

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
        validate_variant_column_agreement(scores, counts)


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


def validate_hgvs_columns(df: pd.DataFrame, target_seq: str, target_seq_type: str = "infer") -> None:
    """Validate the contents of the HGVS variant columns.

    This function assumes that the validate_column_names validator has already been passed.

    Parameters
    ----------
    df : pandas.DataFrame
        A scores or counts dataframe
    target_seq : str
        The target sequence to validate the variants against
    target_seq_type : str
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
    if target_seq_type == "infer":
        target_seq_type = infer_sequence_type(target_seq)

    if target_seq_type not in ("dna", "protein"):
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

    # test prefix consistency for splice (must be "c." or "n.")
    if hgvs_splice_column in hgvs_columns:
        strings = df[hgvs_columns[hgvs_splice_column]].dropna()
        if len(set(s[:2] for s in strings)) > 1:
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_splice_column]}' has inconsistent variant prefixes")
        elif not all(s[:2] in ("c.", "n.") for s in strings):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_splice_column]}' has invalid variant prefixes")

    # test prefix consistency for nt (must be "cngmo." and consistent)
    if hgvs_nt_column in hgvs_columns:
        prefixes = [f"{a}." for a in "cngmo"]
        strings = df[hgvs_columns[hgvs_nt_column]].dropna()
        if len(set(s[:2] for s in strings)) > 1:
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' has inconsistent variant prefixes")
        elif not all(s[:2] in prefixes for s in strings):
            raise DataframeValidationError(f"variant column '{hgvs_columns[hgvs_nt_column]}' has invalid variant prefixes")

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
        for i, s in df[hgvs_columns[hgvs_nt_column]].items():
            if s is not None:
                try:
                    Variant(s, targetseq=target_seq)
                except MaveHgvsParseError:
                    try:
                        Variant(s)
                    except MaveHgvsParseError:
                        invalid_variants.append(f"invalid variant string '{s}' at row {i}")
                    else:
                        invalid_variants.append(f"target sequence mismatch for '{s}' at row {i}")

    if hgvs_pro_column in hgvs_columns:
        if target_seq_type == "dna":
            target_seq = translate_dna(target_seq)[0]
        for i, s in df[hgvs_columns[hgvs_pro_column]].items():
            if s is not None:
                try:
                    Variant(s, targetseq=target_seq)
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


def validate_hgvs_column(column: pd.Series, is_index: bool, target_seq: str, target_seq_type: str = "infer") -> None:
    """
    Validate the variants in an HGVS column from a dataframe.

    Tests whether the column has a correct and consistent prefix.
    This function also validates all individual variants in the column and checks for agreement against the target
    sequence (for non-splice variants).

    Parameters
    ----------
    column : pd.Series
        The column from the dataframe to validate
    is_index : bool
        True if this is the index column for the dataframe and therefore cannot have missing values; else False
    target_seq : str
        The target sequence to validate against
    target_seq_type : str
        The type of target sequence, which can be one of "dna", "protein", or "infer"

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the target sequence does is not dna or protein (or inferred as dna or protein)
    ValueError
        If the target sequence is not valid for the variants (e.g. protein sequence for nucleotide variants)
    ValueError
        If the column name is not recognized
    DataframeValidationError
        If the column contains multiple prefixes or the wrong prefix for that column name
    DataframeValidationError
        If an index column contains missing values
    DataframeValidationError
        If one of the variants fails validation
    """
    if target_seq_type == "infer":
        target_seq_type = infer_sequence_type(target_seq)
    if target_seq_type not in ("dna", "protein"):
        raise ValueError("invalid target sequence type")

    if infer_column_type(column) != "string":
        raise DataframeValidationError(f"variant column '{column.name}' cannot contain numeric data")

    if is_index:
        if column.isna().any():
            raise DataframeValidationError(f"primary variant column '{column.name}' cannot contain null values")
        if not column.is_unique:
            raise DataframeValidationError(f"primary variant column '{column.name}' must contain unique values")

    # check the variant prefixes
    if column.name.lower() == hgvs_nt_column:
        prefixes = [f"{a}." for a in "cngmo"]
    elif column.name.lower() == hgvs_splice_column:
        prefixes = [f"{a}." for a in "cn"]
    elif column.name.lower() == hgvs_pro_column:
        prefixes = ["p."]
    else:
        raise ValueError(f"unrecognized hgvs column name '{column.name}'")
    if len(set(s[:2] for s in column.dropna())) > 1:
        raise DataframeValidationError(f"variant column '{column.name}' has inconsistent variant prefixes")
    if not all(s[:2] in prefixes for s in column.dropna()):
        raise DataframeValidationError(f"variant column '{column.name}' has invalid variant prefixes")

    # validate the individual variant strings
    # prepare the target sequence for validation
    if column.name.lower() == hgvs_nt_column:
        if target_seq_type != "dna":
            raise ValueError(f"invalid target sequence type for '{column.name}'")
    elif column.name.lower() == hgvs_splice_column:
        target_seq = None  # don't validate splice variants against the target sequence
    elif column.name.lower() == hgvs_pro_column:
        if target_seq_type == "dna":
            target_seq = translate_dna(target_seq)[0]  # translate the target sequence if needed
    else:
        raise ValueError(f"unrecognized hgvs column name '{column.name}'")
    # get a list of all invalid variants
    invalid_variants = list()
    for i, s in column.items():
        if s is not None:
            try:
                Variant(s, targetseq=target_seq)
            except MaveHgvsParseError:
                try:
                    Variant(s)  # note this will get called a second time for splice variants
                except MaveHgvsParseError:
                    invalid_variants.append(f"invalid variant string '{s}' at row {i}")
                else:
                    invalid_variants.append(f"target sequence mismatch for '{s}' at row {i}")
    # format and raise an error message that contains all invalid variants
    if len(invalid_variants) > 0:
        raise DataframeValidationError(f"encountered {len(invalid_variants)} invalid variant strings: {(', '.join(invalid_variants))}")


def validate_hgvs_prefix_combinations(hgvs_nt: Optional[str], hgvs_splice: Optional[str], hgvs_pro: Optional[str]) -> None:
    """
    Validate the combination of HGVS variant prefixes.

    This function assumes that other validation, such as checking that all variants in the column have the same prefix,
    has already been performed.

    Parameters
    ----------
    hgvs_nt : Optional[str]
        The first character (prefix) of the HGVS nucleotide variant strings, or None if not used.
    hgvs_splice : Optional[str]
        The first character (prefix) of the HGVS splice variant strings, or None if not used.
    hgvs_pro : Optional[str]
        The first character (prefix) of the HGVS protein variant strings, or None if not used.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If upstream validation failed and an invalid prefix string was passed to this function
    DataframeValidationError
        If the combination of prefixes is not valid
    """
    # ensure that the prefixes are valid - this validation should have been performed before this function was called
    if hgvs_nt not in list("cngmo") + [None]:
        raise ValueError("invalid nucleotide prefix")
    if hgvs_splice not in list("cn") + [None]:
        raise ValueError("invalid nucleotide prefix")
    if hgvs_pro not in ["p", None]:
        raise ValueError("invalid protein prefix")

    # test agreement of prefixes across columns
    if hgvs_splice is not None:
        if hgvs_nt not in list("gmo"):
            raise DataframeValidationError("nucleotide variants must use valid genomic prefix when splice variants are present")
        if hgvs_pro is not None:
            if hgvs_splice != "c":
                raise DataframeValidationError("splice variants' must use 'c.' prefix when protein variants are present")
        else:
            if hgvs_splice != "n":
                raise DataframeValidationError("splice variants must use 'n.' prefix when protein variants are not present")
    elif hgvs_pro is not None and hgvs_nt is not None:
        if hgvs_nt != "c":
            raise DataframeValidationError("nucleotide variants must use 'c.' prefix when protein variants are present and splicing variants are not present")
    elif hgvs_nt is not None:  # just hgvs_nt
        if hgvs_nt != "n":
            raise DataframeValidationError("nucleotide variants must use 'n.' prefix when only nucleotide variants are defined")


def validate_variant_combinations(df: pd.DataFrame) -> None:
    """
    Ensure that variants defined in a single row describe the same variant.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    None

    """
    # TODO
    pass


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


def validate_variant_column_agreement(scores, counts):
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
