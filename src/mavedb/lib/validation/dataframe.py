import pandas as pd
from mavehgvs.variant import Variant
from mavehgvs.exceptions import MaveHgvsParseError
import numpy as np
from typing import Optional, Tuple

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_splice_column,
    hgvs_pro_column,
    required_score_column,
)
from mavedb.lib.validation.exceptions import ValidationError
from fqfa.util.translate import translate_dna

# handle with pandas all null strings
# provide a csv or a pandas dataframe
# take dataframe, output as csv to temp directory, use standard library


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


def validate_dataframe(df: pd.DataFrame, kind: str, target_seq: str, target_seq_type: str) -> None:
    """
    Validate that a given dataframe passes all checks.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to validate
    kind : str
        The kind of dataframe "counts" or "scores"
    target_seq : str
        The target sequence to validate variants against
    target_seq_type : str
        The kind of target sequence, one of "infer" "dna" or "protein"

    Returns
    -------
    None

    Raises
    ------
    ValidationError
        If one of the validators called raises an exception
    """
    # basic checks
    validate_column_names(df, kind)
    validate_no_null_rows(df)

    # validate individual columns
    column_mapping = {c.lower(): c for c in df.columns}
    index_column = choose_dataframe_index_column(df)
    for c in column_mapping:
        if c in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            is_index = column_mapping[c] == index_column
            if is_index or not df[column_mapping[c]].isna().all():  # ignore null non-index HGVS columns
                validate_hgvs_column(df[column_mapping[c]], is_index, target_seq, target_seq_type)
        else:
            force_numeric = (c == required_score_column) or (kind == "counts")
            validate_data_column(df[column_mapping[c]], force_numeric)

    # validate hgvs agreement
    prefixes = dict()
    for c in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
        prefixes[c] = None
        if c in column_mapping:
            if not df[column_mapping[c]].isna().all():
                prefixes[c] = df[column_mapping[c]].dropna()[0][0]
    validate_hgvs_prefix_combinations(
        hgvs_nt=prefixes[hgvs_nt_column], hgvs_splice=prefixes[hgvs_splice_column], hgvs_pro=prefixes[hgvs_pro_column]
    )


def validate_and_standardize_dataframe_pair(
    scores_df: pd.DataFrame, counts_df: Optional[pd.DataFrame], target_seq: str, target_seq_type: str
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Perform validation and standardization on a pair of score and count dataframes.

    Parameters
    ----------
    scores_df : pandas.DataFrame
        The scores dataframe
    counts_df : Optional[pandas.DataFrame]
        The counts dataframe, can be None if not present
    target_seq : str
        The target sequence for the dataset
    target_seq_type : str
        The target sequence type, can be "infer" "dna" or "protein"

    Returns
    -------
    Tuple[pd.DataFrame, Optional[pd.DataFrame]]
        The standardized score and count dataframes, or score and None if no count dataframe was provided

    Raises
    ------
    ValidationError
        If one of the validation functions raises an exception
    """
    # validate the dataframes
    validate_dataframe(scores_df, "scores", target_seq, target_seq_type)
    if counts_df is not None:
        validate_dataframe(counts_df, "counts", target_seq, target_seq_type)
        validate_variant_columns_match(scores_df, counts_df)

    new_scores_df = standardize_dataframe(scores_df)
    if counts_df is not None:
        new_counts_df = standardize_dataframe(counts_df)
    else:
        new_counts_df = None
    return new_scores_df, new_counts_df


def choose_dataframe_index_column(df: pd.DataFrame) -> str:
    """
    Identify the HGVS variant column that should be used as the index column in this dataframe.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to check

    Returns
    -------
    str
        The column name of the index column

    Raises
    ------
    ValidationError
        If no valid HGVS variant column is found
    """
    column_mapping = {c.lower(): c for c in df.columns if not df[c].isna().all()}

    if hgvs_nt_column in column_mapping:
        return column_mapping[hgvs_nt_column]
    elif hgvs_pro_column in column_mapping:
        return column_mapping[hgvs_pro_column]
    else:
        raise ValidationError("failed to find valid HGVS variant column")


def validate_no_null_rows(df: pd.DataFrame) -> None:
    """Check that there are no fully null rows in the dataframe.

    Parameters
    __________
    df : pandas.DataFrame
        The scores or counts dataframe being validated

    Raises
    ______
    ValidationError
        If there are null rows in the dataframe
    """
    null_rows = df.apply(lambda row: np.all(row.isna()), axis=1)
    if sum(null_rows) > 0:
        raise ValidationError(f"found {sum(null_rows)} null rows in the data frame")


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
    ValidationError
        If the column names are not valid
    """
    if any(type(c) != str for c in df.columns):
        raise ValidationError("column names must be strings")

    if any(c.isspace() for c in df.columns) or any(len(c) == 0 for c in df.columns):
        raise ValidationError("column names cannot be empty or whitespace")

    columns = [c.lower() for c in df.columns]

    if kind == "scores":
        if required_score_column not in columns:
            raise ValidationError(f"score dataframe must have a '{required_score_column}' column")
    elif kind == "counts":
        if required_score_column in columns:
            raise ValidationError(f"counts dataframe must not have a '{required_score_column}' column")
    else:
        raise ValueError("kind only accepts scores and counts")

    if hgvs_splice_column in columns:
        if hgvs_nt_column not in columns or hgvs_pro_column not in columns:
            raise ValidationError(
                f"dataframes with '{hgvs_splice_column}' must also define '{hgvs_nt_column}' and '{hgvs_pro_column}'"
            )

    if len(columns) != len(set(columns)):
        raise ValidationError("duplicate column names are not allowed (this check is case insensitive)")

    if set(columns).isdisjoint({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column}):
        raise ValidationError("dataframe does not define any variant columns")

    if set(columns).issubset({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column}):
        raise ValidationError("dataframe does not define any data columns")


def validate_hgvs_column(column: pd.Series, is_index: bool, target_seq: str, target_seq_type) -> None:
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
    ValidationError
        If the column contains multiple prefixes or the wrong prefix for that column name
    ValidationError
        If an index column contains missing values
    ValidationError
        If one of the variants fails validation
    """
    if target_seq_type not in ("dna", "protein"):
        raise ValueError("invalid target sequence type")

    if infer_column_type(column) not in ("string", "empty"):
        raise ValidationError(f"variant column '{column.name}' cannot contain numeric data")

    if is_index:
        if column.isna().any():
            raise ValidationError(f"primary variant column '{column.name}' cannot contain null values")
        if not column.is_unique:
            raise ValidationError(f"primary variant column '{column.name}' must contain unique values")

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
        raise ValidationError(f"variant column '{column.name}' has inconsistent variant prefixes")
    if not all(s[:2] in prefixes for s in column.dropna()):
        raise ValidationError(f"variant column '{column.name}' has invalid variant prefixes")

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
        raise ValidationError(
            f"encountered {len(invalid_variants)} invalid variant strings: {(', '.join(invalid_variants))}"
        )


def validate_hgvs_prefix_combinations(
    hgvs_nt: Optional[str], hgvs_splice: Optional[str], hgvs_pro: Optional[str]
) -> None:
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
    ValidationError
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
            raise ValidationError("nucleotide variants must use valid genomic prefix when splice variants are present")
        if hgvs_pro is not None:
            if hgvs_splice != "c":
                raise ValidationError("splice variants' must use 'c.' prefix when protein variants are present")
        else:
            if hgvs_splice != "n":
                raise ValidationError("splice variants must use 'n.' prefix when protein variants are not present")
    elif hgvs_pro is not None and hgvs_nt is not None:
        if hgvs_nt != "c":
            raise ValidationError(
                "nucleotide variants must use 'c.' prefix when protein variants are present and splicing variants are"
                " not present"
            )
    elif hgvs_nt is not None:  # just hgvs_nt
        if hgvs_nt != "n":
            raise ValidationError("nucleotide variants must use 'n.' prefix when only nucleotide variants are defined")


def validate_variant_consistency(df: pd.DataFrame) -> None:
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


def validate_variant_columns_match(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Checks if two dataframes have matching HGVS columns.

    The check performed is order-independent.
    This function is used to validate a pair of scores and counts dataframes that were uploaded together.

    Parameters
    ----------
    df1 : pandas.DataFrame
        Dataframe parsed from an uploaded scores file
    df2 : pandas.DataFrame
        Dataframe parsed from an uploaded counts file

    Raises
    ------
    ValidationError
        If both dataframes do not define the same variant columns
    ValidationError
        If both dataframes do not define the same variants within each column
    """
    for c in df1.columns:
        if c.lower() in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            if c not in df2:
                raise ValidationError("both score and count dataframes must define matching HGVS columns")
            elif df1[c].isnull().all() and df2[c].isnull().all():
                continue
            elif np.any(df1[c].sort_values().values != df2[c].sort_values().values):
                raise ValidationError(
                    f"both score and count dataframes must define matching variants, discrepancy found in '{c}'"
                )
    for c in df2.columns:
        if c.lower() in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            if c not in df1:
                raise ValidationError("both score and count dataframes must define matching HGVS columns")
