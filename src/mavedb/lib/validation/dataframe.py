from typing import Optional, Tuple, Union

import hgvs.exceptions
import hgvs.parser
import hgvs.validator
import numpy as np
import pandas as pd
from cdot.hgvs.dataproviders import RESTDataProvider
from fqfa.util.translate import translate_dna
from mavehgvs.exceptions import MaveHgvsParseError
from mavehgvs.variant import Variant

from mavedb.lib.exceptions import MixedTargetError
from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence

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
    column_mapper = {x: x.lower() for x in df.columns if x.lower() in STANDARD_COLUMNS}

    df.rename(columns=column_mapper, inplace=True)

    return sort_dataframe_columns(df)


def validate_and_standardize_dataframe_pair(
    scores_df: pd.DataFrame, counts_df: Optional[pd.DataFrame], targets: list[TargetGene], hdp: RESTDataProvider
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


def validate_dataframe(df: pd.DataFrame, kind: str, targets: list["TargetGene"], hdp: RESTDataProvider) -> None:
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

    column_mapping = {c.lower(): c for c in df.columns}
    index_column = choose_dataframe_index_column(df)

    prefixes: dict[str, Optional[str]] = dict()
    for c in column_mapping:
        if c in (hgvs_nt_column, hgvs_splice_column, hgvs_pro_column):
            is_index = column_mapping[c] == index_column
            prefixes[c] = None

            # Ignore validation for null non-index hgvs columns
            if df[column_mapping[c]].isna().all() and not is_index:
                continue

            score_set_is_accession_based = all(target.target_accession for target in targets)
            score_set_is_sequence_based = all(target.target_sequence for target in targets)

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

        else:
            force_numeric = (c == required_score_column) or (kind == "counts")
            validate_data_column(df[column_mapping[c]], force_numeric)

    validate_hgvs_prefix_combinations(
        hgvs_nt=prefixes[hgvs_nt_column],
        hgvs_splice=prefixes[hgvs_splice_column],
        hgvs_pro=prefixes[hgvs_pro_column],
        transgenic=all(target.target_sequence for target in targets),
    )


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


def validate_hgvs_transgenic_column(column: pd.Series, is_index: bool, targets: dict[str, "TargetSequence"]) -> None:
    """
    Validate the variants in an HGVS column from a dataframe.

    Tests whether the column has a correct and consistent prefix.
    This function also validates all individual variants in the column and checks for agreement against the target
    sequence (for non-splice variants).

    Implementation NOTE: We assume variants will only be presented as fully qualified (accession:variant)
    if this column is being validated against multiple targets.

    Parameters
    ----------
    column : pd.Series
        The column from the dataframe to validate
    is_index : bool
        True if this is the index column for the dataframe and therefore cannot have missing values; else False
    targets : dict
        Dictionary containing a mapping of target gene names to their sequences.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the target sequence does is not dna or protein (or inferred as dna or protein)
    ValueError
        If the target sequence is not valid for the variants (e.g. protein sequence for nucleotide variants)
    ValidationError
        If one of the variants fails validation
    """
    valid_sequence_types = ("dna", "protein")
    validate_variant_column(column, is_index)
    prefixes = generate_variant_prefixes(column)
    validate_variant_formatting(column, prefixes, list(targets.keys()), len(targets) > 1)

    observed_sequence_types = [target.sequence_type for target in targets.values()]
    invalid_sequence_types = set(observed_sequence_types) - set(valid_sequence_types)
    if invalid_sequence_types:
        raise ValueError(
            f"Some targets are invalid sequence types: {invalid_sequence_types}. Sequence types shoud be one of: {valid_sequence_types}"
        )

    # If this is the `hgvs_nt` column, at least one target should be of type `dna`.
    if str(column.name).lower() == hgvs_nt_column:
        if "dna" not in observed_sequence_types:
            raise ValueError(
                f"invalid target sequence type(s) for '{column.name}'. At least one target should be of type `dna`. Observed types: {observed_sequence_types}"
            )

    # Make sure this column is either the splice column or protein column.
    elif str(column.name).lower() != hgvs_splice_column and str(column.name).lower() != hgvs_pro_column:
        raise ValueError(f"unrecognized hgvs column name '{column.name}'")

    # Build dictionary of target sequences based on the column we are validating.
    target_seqs: dict[str, Union[str, None]] = {}
    for name, target in targets.items():
        if str(column.name).lower() == hgvs_nt_column:
            target_seqs[name] = target.sequence

        # don't validate splice columns against provided sequences.
        elif str(column.name).lower() == hgvs_splice_column:
            target_seqs[name] = None

        # translate the target sequence if needed.
        elif str(column.name).lower() == hgvs_pro_column:
            if target.sequence_type == "dna" and target.sequence is not None:
                target_seqs[name] = translate_dna(target.sequence)[0]
            else:
                target_seqs[name] = target.sequence

    # get a list of all invalid variants
    invalid_variants = list()
    for i, s in column.items():
        if not s:
            continue

        # variants can exist on the same line separated by a space
        for variant in s.split(" "):
            # When there are multiple targets, treat provided variants as fully qualified.
            if len(targets) > 1:
                name, variant = str(variant).split(":")
            else:
                name = list(targets.keys())[0]
            if variant is not None:
                try:
                    Variant(variant, targetseq=target_seqs[name])
                except MaveHgvsParseError:
                    try:
                        Variant(variant)  # note this will get called a second time for splice variants
                    except MaveHgvsParseError:
                        invalid_variants.append(f"invalid variant string '{variant}' at row {i} for sequence {name}")
                    else:
                        invalid_variants.append(
                            f"target sequence mismatch for '{variant}' at row {i} for sequence {name}"
                        )

    # format and raise an error message that contains all invalid variants
    if len(invalid_variants) > 0:
        raise ValidationError(
            f"encountered {len(invalid_variants)} invalid variant strings.", triggers=invalid_variants
        )


def validate_hgvs_genomic_column(
    column: pd.Series, is_index: bool, targets: list["TargetAccession"], hdp: RESTDataProvider
) -> None:
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
    targets : list
        Dictionary containing a list of target accessions.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the target sequence does is not dna or protein (or inferred as dna or protein)
    ValueError
        If the target sequence is not valid for the variants (e.g. protein sequence for nucleotide variants)
    ValidationError
        If one of the variants fails validation
    """
    validate_variant_column(column, is_index)
    prefixes = generate_variant_prefixes(column)
    validate_variant_formatting(
        column, prefixes, [target.accession for target in targets if target.accession is not None], True
    )

    # validate the individual variant strings
    # prepare the target sequences for validation
    target_seqs: dict[str, Union[str, None]] = {}
    for target in targets:
        assert target.accession is not None
        # We shouldn't have to worry about translating protein sequences when we deal with accession based variants
        if str(column.name).lower() == hgvs_nt_column or str(column.name).lower() == hgvs_pro_column:
            target_seqs[target.accession] = target.accession

        # TODO: no splice col for genomic coordinate variants?
        elif str(column.name).lower() == hgvs_splice_column:
            target_seqs[target.accession] = None  # don't validate splice variants against a target sequence

        else:
            raise ValueError(f"unrecognized hgvs column name '{column.name}'")

    hp = hgvs.parser.Parser()
    vr = hgvs.validator.Validator(hdp=hdp)

    invalid_variants = list()
    for i, s in column.items():
        if s is not None:
            for variant in s.split(" "):
                try:
                    # We set strict to `False` to suppress validation warnings about intronic variants.
                    vr.validate(hp.parse(variant), strict=False)
                except hgvs.exceptions.HGVSError as e:
                    invalid_variants.append(f"Failed to parse row {i} with HGVS exception: {e}")

    # format and raise an error message that contains all invalid variants
    if len(invalid_variants) > 0:
        raise ValidationError(
            f"encountered {len(invalid_variants)} invalid variant strings.", triggers=invalid_variants
        )


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
        if not all(len(str(v).split(":")) == 2 for v in variants):
            raise ValidationError(
                f"variant column '{column.name}' needs fully qualified coordinates when validating against multiple targets"
            )
        if len(set(str(v).split(":")[1][:2] for v in variants)) > 1:
            raise ValidationError(f"variant column '{column.name}' has inconsistent variant prefixes")
        if not all(str(v).split(":")[1][:2] in prefixes for v in variants):
            raise ValidationError(f"variant column '{column.name}' has invalid variant prefixes")
        if not all(str(v).split(":")[0] in targets for v in variants):
            raise ValidationError(f"variant column '{column.name}' has invalid accession identifiers")

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


def validate_hgvs_prefix_combinations(
    hgvs_nt: Optional[str], hgvs_splice: Optional[str], hgvs_pro: Optional[str], transgenic: bool
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
    transgenic : bool
        Whether we should validate these prefix combinations as transgenic variants

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
    # Only raise if this data will not be validated by biocommons.hgvs
    elif hgvs_nt is not None:  # just hgvs_nt
        if hgvs_nt != "n" and transgenic:
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
