from typing import Optional, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd

from mavedb.lib.exceptions import MixedTargetError
from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    guide_sequence_column,
    required_score_column,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.target_gene import TargetGene
from mavedb.lib.validation.dataframe.column import validate_data_column
from mavedb.lib.validation.dataframe.variant import (
    validate_hgvs_transgenic_column,
    validate_hgvs_genomic_column,
    validate_guide_sequence_column,
    validate_hgvs_prefix_combinations,
)

if TYPE_CHECKING:
    from cdot.hgvs.dataproviders import RESTDataProvider


STANDARD_COLUMNS = (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, required_score_column, guide_sequence_column)


def validate_and_standardize_dataframe_pair(
    scores_df: pd.DataFrame,
    counts_df: Optional[pd.DataFrame],
    targets: list[TargetGene],
    hdp: Optional["RESTDataProvider"],
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Perform validation and standardization on a pair of score and count dataframes.

    Parameters
    ----------
    scores_df : pandas.DataFrame
        The scores dataframe
    counts_df : Optional[pandas.DataFrame]
        The counts dataframe, can be None if not present
    targets : str
        The target genes on which to validate dataframes
    hdp : RESTDataProvider
        The biocommons.hgvs compatible data provider. Used to fetch sequences for hgvs validation.

    Returns
    -------
    Tuple[pd.DataFrame, Optional[pd.DataFrame]]
        The standardized score and count dataframes, or score and None if no count dataframe was provided

    Raises
    ------
    ValidationError
        If one of the validation functions raises an exception
    """
    if not targets:
        raise ValueError("Can't validate provided file with no targets.")

    validate_dataframe(scores_df, "scores", targets, hdp)
    if counts_df is not None:
        validate_dataframe(counts_df, "counts", targets, hdp)
        validate_variant_columns_match(scores_df, counts_df)

    new_scores_df = standardize_dataframe(scores_df)
    new_counts_df = standardize_dataframe(counts_df) if counts_df is not None else None
    return new_scores_df, new_counts_df


def validate_dataframe(
    df: pd.DataFrame, kind: str, targets: list["TargetGene"], hdp: Optional["RESTDataProvider"]
) -> None:
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
    # basic target meta data
    score_set_is_accession_based = all(target.target_accession for target in targets)
    score_set_is_sequence_based = all(target.target_sequence for target in targets)
    score_set_is_base_editor = score_set_is_accession_based and all(
        target.target_accession.is_base_editor for target in targets
    )

    # basic checks
    validate_column_names(df, kind, score_set_is_base_editor)
    validate_no_null_rows(df)

    column_mapping = {c.lower(): c for c in df.columns}
    index_column = choose_dataframe_index_column(df, score_set_is_base_editor)

    prefixes: dict[str, Optional[str]] = dict()
    for c in column_mapping:
        is_index = column_mapping[c] == index_column

        if c in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            prefixes[c] = None

            # Ignore validation for null non-index hgvs columns
            if df[column_mapping[c]].isna().all() and not is_index:
                continue

            # This is typesafe, despite Pylance's claims otherwise
            if score_set_is_accession_based and not score_set_is_sequence_based:
                validate_hgvs_genomic_column(
                    df[column_mapping[c]],
                    is_index,
                    [target.target_accession for target in targets],
                    hdp,  # type: ignore
                )
            elif score_set_is_sequence_based and not score_set_is_accession_based:
                validate_hgvs_transgenic_column(
                    df[column_mapping[c]],
                    is_index,
                    {target.target_sequence.label: target.target_sequence for target in targets},  # type: ignore
                )
            else:
                raise MixedTargetError("Could not validate dataframe against provided mixed target types.")

            # post validation, handle prefixes. We've already established these columns are non-null
            if score_set_is_accession_based or len(targets) > 1:
                prefixes[c] = (
                    df[column_mapping[c]].dropna()[0].split(" ")[0].split(":")[1][0]
                )  # Just take the first prefix, we validate consistency elsewhere
            else:
                prefixes[c] = df[column_mapping[c]].dropna()[0][0]

        elif c == guide_sequence_column:
            validate_guide_sequence_column(df[column_mapping[c]], is_index=is_index)

        else:
            force_numeric = (c == required_score_column) or (kind == "counts")
            validate_data_column(df[column_mapping[c]], force_numeric)

    validate_hgvs_prefix_combinations(
        hgvs_nt=prefixes[hgvs_nt_column],
        hgvs_splice=prefixes[hgvs_splice_column],
        hgvs_pro=prefixes[hgvs_pro_column],
        transgenic=score_set_is_sequence_based,
    )


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
    column_mapper = {x: x.lower() for x in df.columns if x.lower() in STANDARD_COLUMNS}

    df.rename(columns=column_mapper, inplace=True)

    return sort_dataframe_columns(df)


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


def validate_column_names(df: pd.DataFrame, kind: str, is_base_editor: bool) -> None:
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
    if any(type(c) is not str for c in df.columns):
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
        msg = "dataframes with '{0}' must also define a '{1}' column"
        if hgvs_nt_column not in columns:
            raise ValidationError(msg.format(hgvs_splice_column, hgvs_nt_column))
        elif hgvs_pro_column not in columns:
            raise ValidationError(msg.format(hgvs_splice_column, hgvs_pro_column))

    if len(columns) != len(set(columns)):
        raise ValidationError("duplicate column names are not allowed (this check is case insensitive)")

    if is_base_editor:
        msg = "dataframes for base editor data must also define the '{0}' column"
        if guide_sequence_column not in columns:
            raise ValidationError(msg.format(guide_sequence_column))

        elif hgvs_nt_column not in columns:
            raise ValidationError(msg.format(hgvs_nt_column))

    if set(columns).isdisjoint({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column}):
        raise ValidationError("dataframe does not define any variant columns")

    if set(columns).issubset({hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, guide_sequence_column}):
        raise ValidationError("dataframe does not define any data columns")


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
    if any(df.isnull().all(axis=1)):
        raise ValidationError(f"found {len(df[df.isnull().all(axis=1)])} null rows in the data frame")


def choose_dataframe_index_column(df: pd.DataFrame, is_base_editor: bool) -> str:
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

    if is_base_editor:
        return column_mapping[guide_sequence_column]
    elif hgvs_nt_column in column_mapping:
        return column_mapping[hgvs_nt_column]
    elif hgvs_pro_column in column_mapping:
        return column_mapping[hgvs_pro_column]
    else:
        raise ValidationError("failed to find valid HGVS variant column")


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
        if c.lower() in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, guide_sequence_column):
            if c not in df2:
                raise ValidationError("both score and count dataframes must define matching HGVS columns")
            elif df1[c].isnull().all() and df2[c].isnull().all():
                continue
            elif np.any(df1[c].sort_values().values != df2[c].sort_values().values):
                raise ValidationError(
                    f"both score and count dataframes must define matching variants, discrepancy found in '{c}'"
                )
    for c in df2.columns:
        if c.lower() in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, guide_sequence_column):
            if c not in df1:
                raise ValidationError("both score and count dataframes must define matching HGVS columns")
