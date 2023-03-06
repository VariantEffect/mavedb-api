from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from mavehgvs.variant import Variant
import numpy as np

from src.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column
)
from src.lib.validation.exceptions import ValidationError
from src.lib.validation.variant import validate_hgvs_string
from src.lib.validation.utilities import convert_hgvs_nt_to_hgvs_pro
from src.lib.validation.target import validate_target_sequence
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


def validate_dataframes(target_seq: str, scores, counts=None):
    """
    Validates scores and counts dataframes for MaveDB upload. This function performs
    comprehensive validation.

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
    scores = validate_column_names(scores)
    validate_values_by_column(scores, target_seq)
    if counts is not None:
        #validate_no_null_columns_or_rows(counts)
        counts = validate_column_names(counts, scores=False)
        validate_values_by_column(counts, target_seq)
        validate_dataframes_define_same_variants(scores, counts)


def validate_no_null_columns_or_rows(dataframe):
    # TODO: we may not need this - current datasets exist where all values are None
    """
    Checks that there are no null columns or rows in the dataframe. Note that a null
    column may still have a valid column name.

    Parameters
    __________
    dataframe : pandas.DataFrame
        The scores or counts dataframe being validated

    Raises
    ______
    ValidationError
        If there are null columns or rows in the dataframe
    """
    # first drop any columns where null columns are allowed
    if hgvs_nt_column:
        dataframe = dataframe.drop([hgvs_nt_column], axis=1)
    if hgvs_pro_column:
        dataframe = dataframe.drop([hgvs_pro_column], axis=1)
    if hgvs_splice_column:
        dataframe = dataframe.drop([hgvs_splice_column], axis=1)
    df = dataframe.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
    try:
        assert_frame_equal(df, dataframe)
    except AssertionError:
        raise ValidationError("Dataset should not contain null columns or rows.")


def validate_column_names(dataframe, scores=True):
    # TODO: return errors to user regarding column name ordering
    """
    This function validates the columns in a dataframe. The first columns should be
    an hgvs column such as hgvs_nt, hgvs_pro, and hgvs_splice. There should be at least
    one column beyond the hgvs columns. A scores dataframe should have a score column and
    a counts dataframe should have a counts column. There should not be any null columns.
    The column names will also be validated against unusual file conversions that could
    corrupt the column names.

    Parameters
    __________
    dataframe : pandas.DataFrame
        The scores or counts dataframe to be validated.

    Raises
    ______
    ValidationError
        If the column names are not formatted correctly.
    """
    # get columns from dataframe
    columns = dataframe.columns

    # check that there are no duplicate column names
    # This one seems useless cause duplicate column names will be processed automatically
    if len(columns) != len(set(columns)):
        raise ValidationError("There cannot be duplicate column names.")

    # count instances of hgvs columns
    count = 0
    # note presence of different columns
    hgvs_nt = False
    hgvs_pro = False
    hgvs_splice = False
    score_column = False
    for i in range(len(columns)):
        # there should not be any null columns
        # check for empty strings, np.nan, and None
        # if is_null(columns[i]) or columns[i] is None:
        if not isinstance(columns[i], str) or columns[i] == "" or columns[i].isspace():
            # above condition will check that value is not None or np.nan also
            raise ValidationError("Column names must not be null.")  # in readable_null_values_list:
        if columns[i] in [hgvs_nt_column, hgvs_pro_column, hgvs_splice_column]:
            count += 1

    for column in dataframe.columns:
        if len(dataframe.index) != dataframe[column].isnull().sum():
            if column == hgvs_nt_column:
                hgvs_nt = True
            elif column == hgvs_pro_column:
                hgvs_pro = True
            elif column == hgvs_splice_column:
                hgvs_splice = True
            elif column == required_score_column:
                score_column = True
            elif (column == hgvs_nt_column.upper() or
                  column == hgvs_pro_column.upper() or
                  column == hgvs_splice_column.upper() or
                  column == required_score_column.upper()):
                raise ValidationError("hgvs columns and score column should be lowercase.")

    # there should be at least one of hgvs_nt or hgvs_pro column
    # if count == 0:
    if not hgvs_nt and not hgvs_pro:
        raise ValidationError("Must include hgvs_nt or hgvs_pro column.")  # or hgvs_splice column.")

    # splice should not be defined in nt is not
    if hgvs_splice and not hgvs_nt:
        raise ValidationError("Must define hgvs_nt column if defining hgvs_splice column.")

    # first columns should be hgvs columns, reorder columns to meet this requirement
    if score_column:
        score = dataframe.pop(required_score_column)
        dataframe.insert(0, required_score_column, score)
    if hgvs_splice:
        splice_column = dataframe.pop(hgvs_splice_column)
        dataframe.insert(0, hgvs_splice_column, splice_column)
    if hgvs_pro:
        pro_column = dataframe.pop(hgvs_pro_column)
        dataframe.insert(0, hgvs_pro_column, pro_column)
    if hgvs_nt:
        nt_column = dataframe.pop(hgvs_nt_column)
        dataframe.insert(0, hgvs_nt_column, nt_column)

    # there should be at least one additional column beyond the hgvs columns
    if len(columns) == count:
        raise ValidationError("There must be at least one additional column beyond the hgvs columns.")

    # if dataframe is a scores df make sure it has a score column
    # also make sure counts df has a counts column and not a score column
    if scores and not score_column:
        raise ValidationError("A scores dataframe must include a `score` column.")
    if not scores and score_column:
        raise ValidationError("A counts dataframe should not include a `score` column, include `score` "
                              "column in a scores dataframe.")

    return dataframe


def validate_values_by_column(dataset, target_seq: str):
    """
    Validates that the values in each column labeled `hgvs_nt`, `hgvs_pro`, `hgvs_splice`, and `score` make sense
    with regards to their column name. It also validates via a helper function that if both an `hgvs_nt` column and
    an `hgvs_pro` column exist, they are consistent with one another.

    Parameters
    __________
    dataset : pandas.DataFrame
        A scores or counts dataframe.
    target_seq: str
        The hgvs column name from which the variants parameter originates.

    Raises
    ______
    ValidationError
        If the target sequence does not contain solely the bases ACTG.
    ValidationError
        If any variant fails validation or if the variants are not consistent with one another.
    """
    # first check that dataframe is not empty
    if dataset.empty:
        raise ValidationError("Dataset must not be empty.")

    # validate target sequence
    validate_target_sequence(target_seq)

    # first check the column names, establish the order or the hgvs and score columns
    hgvs_nt = False
    hgvs_pro = False
    hgvs_splice = False
    score = False
    # Every dataset will have hgvs_nt, pro and splice columns, but their values may be NaN.
    for column in dataset.columns:
        if len(dataset.index) != dataset[column].isnull().sum():
            if column == hgvs_nt_column:
                hgvs_nt = True
            elif column == hgvs_pro_column:
                hgvs_pro = True
            elif column == hgvs_splice_column:
                hgvs_splice = True
            elif column == required_score_column:
                score = True
    if hgvs_nt is False and hgvs_pro is False and hgvs_splice is False and score is False:
        raise ValidationError("Missing required hgvs and/or score columns.")

    # check that the first column, hgvs_nt or hgvs_pro, is valid
    if hgvs_nt:
        validate_index_column(dataset["hgvs_nt"], hgvs="nt")
    elif hgvs_pro:
        validate_index_column(dataset["hgvs_pro"], hgvs="pro")
    else:
        raise ValidationError("Must include either hgvs_nt or hgvs_pro column.")

    # check that prefixes all match and are consistent with one another
    hgvs_nt_prefix = None

    sequence_type = infer_sequence_type(target_seq)
    if sequence_type=="dna":
        target_dna_seq = target_seq
        target_rna_seq = convert_dna_to_rna(target_seq)
        target_aa_seq = translate_dna(target_seq)[0]
    elif sequence_type=="rna":
        targe_dna_seq = convert_rna_to_dna(target_seq)
        target_rna_seq = target_seq
        target_aa_seq = translate_dna(targe_dna_seq)[0]
    else:
        target_dna_seq = None
        target_rna_seq = None
        target_aa_seq = target_seq

    # loop through row by row, validate hgvs strings, make sure nt and pro are consistent with one another
    # TODO: Haven't finished this part yet. target_seq for hgvs_nt
    for i in range(len(dataset)):
        # NaN type is numpy.float64 so it will be validated.
        if hgvs_nt and dataset.loc[i, hgvs_nt_column] is not None:
            validate_hgvs_string(value=dataset.loc[i, hgvs_nt_column],
                                 column="nt",
                                 targetseq=target_seq,
                                 splice_present=hgvs_splice)
            if hgvs_nt_prefix:
                if Variant(dataset.loc[i, hgvs_nt_column]).prefix != hgvs_nt_prefix:
                    raise ValidationError("All prefixes within the hgvs_nt column must be the same.")
                # if prefix is non-coding, there should not be an hgvs_pro value
                if hgvs_nt_prefix == "n":
                    if hgvs_pro_column and dataset.loc[i, hgvs_pro_column] is not None:
                        raise ValidationError("Cannot have hgvs_pro value with non-coding hgvs_nt value.")
            else:  # assign the prefix value since it has not yet been assigned
                hgvs_nt_prefix = Variant(dataset.loc[i, hgvs_nt_column]).prefix
        if hgvs_pro and dataset.loc[i, hgvs_pro_column] is not None:
            validate_hgvs_string(value=dataset.loc[i, hgvs_pro_column],
                                 column="p",
                                 targetseq=target_aa_seq,
                                 splice_present=hgvs_splice)
        if hgvs_splice and dataset.loc[i, hgvs_splice_column] is not None:
            validate_hgvs_string(value=dataset.loc[i, hgvs_splice_column],
                                 column="splice",
                                 targetseq=target_seq,
                                 splice_present=hgvs_splice)
            if hgvs_nt_prefix != 'g':
                raise ValidationError("hgvs_nt prefix must be genomic when splice present.")
        if score:
            s = validate_score(dataset.loc[i, required_score_column])
            dataset.loc[i, required_score_column] = s
        if hgvs_nt and hgvs_pro and dataset.loc[i, hgvs_nt_column] is not None and dataset.loc[i, hgvs_pro_column] is not None:
            # TODO: ensure this function is implemented correctly before applying, complete unit testing
            if not Variant(dataset.loc[i, hgvs_pro_column]).is_multi_variant():  # can only convert to single hgvs_pro variants
                '''validate_hgvs_nt_and_hgvs_pro_represent_same_change(target_seq=target_seq,
                                                                    nt=dataset.loc[i, hgvs_nt_column],
                                                                    pro=dataset.loc[i, hgvs_pro_column],
                                                                    row=i)'''

        # check that primary column, whether hgvs_nt or hgvs_pro, does not contain None values
    # make sure target seq is the right type
    # no protein target with just nt variants


def validate_index_column(column, hgvs: str):
    """
    Validates the first column in a dataframe, should be hgvs_nt or hgvs_pro. All values in the column should be
    unique and there should be no missing values.

    Parameters
    __________
    column : list
        The column that will be validated.
    hgvs : str
        Indicates whether or not the column is an hgvs_nt or hgvs_pro column. Can have value "nt" or "pro".

    Raises
    ______
    ValidationError
        If there are duplicate values in the column.
    ValidationError
        If there are missing values in the column.
    """
    col_set = set(column)
    if len(col_set) != len(column):
        raise ValidationError(
            "Each value in hgvs_{} column must be unique.".format(hgvs)
        )
    if np.nan in col_set:
        raise ValidationError(
            "Primary column hgvs_{} must not contain missing values.".format(hgvs)
        )


def validate_score(score):
    if isinstance(score, float) or isinstance(score, int):
        pass
    elif isinstance(score, str):
        try:
            return float(score)
        except ValueError:
            raise ValidationError("Each value in score column must be a number. "
                                  "'{}' has the type '{}'.".format(score, type(score).__name__))
    else:
        raise ValidationError("Each value in score column must be a number. "
                              "'{}' has the type '{}'.".format(score, type(score).__name__))


def validate_hgvs_nt_and_hgvs_pro_represent_same_change(target_seq: str, nt: str, pro: str, row: int):
    """
    Checks that, when both an `hgvs_nt` and an `hgvs_pro` exist, the variant strings within
    those columns are representing the same change.

    Parameters
    __________
    target_seq : str
        The target sequence associated withe variants.
    nt : str
        The hgvs_nt string.
    pro : str
        The hgvs_pro string.
    row : int
        The row that the current hgvs strings being evaluated are in.

    Raises
    ______
    ValidationError
        If the variants do not represent the same change.
    """
    # TODO think about how double quoted variants are handled here (e.g., "c.[123A>G;124A>G]")
    nt_converted = convert_hgvs_nt_to_hgvs_pro(nt, target_seq)
    # compare nt_converted with pro
    if nt_converted != pro:
        raise ValidationError("The hgvs_nt variant {} and the hgvs_pro variant {} on row {} do not represent the "
                              "same change.".format(nt, pro, row))


def validate_dataframes_define_same_variants(scores, counts):
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
