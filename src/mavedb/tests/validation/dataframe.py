from unittest import TestCase
import numpy as np
import pandas as pd
from io import StringIO

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column
)
from mavedb.lib.validation.dataframe import (
    validate_no_null_columns_or_rows,
    validate_column_names,
    validate_values_by_column,
    validate_score,
    validate_dataframes_define_same_variants,
    validate_index_column,
    validate_hgvs_nt_and_hgvs_pro_represent_same_change,
)
from mavedb.lib.validation.exceptions import ValidationError


# let pandas handle the types of null values to allow


class TestValidateNoNullColumnsOrRows(TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["c.1A>G"],
                hgvs_pro_column: ["p.Leu5Glu"],
                hgvs_splice_column: ["c.1A>G"],
                required_score_column: 1.0,
            }
        )

    def test_valid(self):
        validate_no_null_columns_or_rows(self.dataframe)

    def test_null_row(self):
        self.dataframe.loc[1] = [np.nan, np.nan, np.nan, np.nan]
        with self.assertRaises(AssertionError):
            validate_no_null_columns_or_rows(self.dataframe)

    def test_null_column(self):
        self.dataframe[required_score_column][0] = np.nan
        with self.assertRaises(AssertionError):
            validate_no_null_columns_or_rows(self.dataframe)


class TestValidateColumnNames(TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["c.1A>G"],
                hgvs_pro_column: ["p.Leu5Glu"],
                hgvs_splice_column: ["c.1A>G"],
                required_score_column: [1.000],
            }
        )

    def test_valid_scores_column_names(self):
        validate_column_names(self.dataframe)

    def test_valid_counts_column_names(self):
        self.dataframe = self.dataframe.drop([required_score_column], axis=1)
        self.dataframe["count"] = [5]
        validate_column_names(self.dataframe, scores=False)

    def test_valid_just_hgvs_nt_hgvs_column(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column, hgvs_splice_column], axis=1)
        validate_column_names(self.dataframe)

    def test_valid_just_hgvs_pro_hgvs_column(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column, hgvs_splice_column], axis=1)
        validate_column_names(self.dataframe)

    def test_primary_column_is_pro_when_nt_is_not_defined(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column, hgvs_splice_column, required_score_column], axis=1)
        self.dataframe.insert(0, required_score_column, [1.000], True)
        self.dataframe = validate_column_names(self.dataframe)
        self.assertTrue(self.dataframe.columns[0] == hgvs_pro_column)

    def test_missing_hgvs_column(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe)

    def test_hgvs_in_wrong_location(self):
        self.dataframe = self.dataframe[[hgvs_nt_column, required_score_column, hgvs_pro_column, hgvs_splice_column]]
        validate_column_names(self.dataframe)  # validation fixes problem, should pass

    def test_no_additional_columns_beyond_hgvs_scores_df(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe)

    def test_no_additional_columns_beyond_hgvs_counts_df(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, scores=False)

    def test_hgvs_columns_must_be_lowercase(self):
        self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}, inplace=True)
        with self.assertRaises(ValueError):
            validate_column_names(self.dataframe)

    def test_duplicate_column_names(self):
        self.dataframe.rename(columns={hgvs_pro_column: hgvs_nt_column}, inplace=True)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe)

    def test_null_column_name(self):
        null_values = [None, np.nan, "", 1, "   "]
        for value in null_values:
            self.dataframe.rename(columns={hgvs_splice_column: value}, inplace=True)
            with self.assertRaises(ValidationError):
                validate_column_names(self.dataframe)

    def test_no_score_column_with_scores_df(self):
        self.dataframe = self.dataframe.drop([required_score_column], axis=1)
        self.dataframe["count"] = [1]
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe)

    def test_no_additional_column_with_counts_df(self):
        self.dataframe = self.dataframe.drop([required_score_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, scores=False)

    def test_invalid_missing_either_required_hgvs_column(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column, hgvs_nt_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, scores=False)

    def test_invalid_splice_column_defined_when_nt_column_is_not(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, scores=False)

    def test_sort_column_names(self):
        self.dataframe = pd.DataFrame(
            {
                "other": 5,
                required_score_column: [1.000],
                hgvs_splice_column: ["c.1A>G"],
                hgvs_pro_column: ["p.Leu5Glu"],
                hgvs_nt_column: ["g.1A>G"],
            }
        )
        dataset = validate_column_names(self.dataframe)
        self.assertTrue(dataset.columns[0] == hgvs_nt_column)
        self.assertTrue(dataset.columns[1] == hgvs_pro_column)
        self.assertTrue(dataset.columns[2] == hgvs_splice_column)
        self.assertTrue(dataset.columns[3] == required_score_column)


class TestValidateValuesByColumn(TestCase):
    def setUp(self):
        self.target_seq = "ATGACA"
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["g.4A>G", "g.5C>G", "g.6A>G"],
                hgvs_pro_column: ["p.Thr2Ala", "p.Thr2Arg", "p.Thr2="],
                hgvs_splice_column: ["c.4A>G", "c.5C>G", "c.6A>G"],
                required_score_column: [1.000, 0.5, 1.5],
            }
        )

    def test_valid(self):
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_non_numeric_values_in_score_column(self):
        self.dataframe.loc[0, [required_score_column]] = "not a float"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_row_hgvs_is_not_a_string(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = 1.0
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_empty_no_variants_parsed(self):
        self.dataframe = self.dataframe.drop(axis='rows', index=[0, 1, 2])
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_hgvs_nt_in_column(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column, hgvs_splice_column], axis=1)
        self.dataframe.loc[0, [hgvs_nt_column]] = "p.Thr1Ala"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_hgvs_pro_in_column(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column, hgvs_splice_column], axis=1)
        self.dataframe.loc[0, [hgvs_pro_column]] = "c.1A>G"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_hgvs_splice_in_column(self):
        self.dataframe = self.dataframe.drop([hgvs_pro_column], axis=1)
        self.dataframe.loc[0, [hgvs_splice_column]] = "g.1A>G"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_variants_do_not_represent_same_change(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.3A>G"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_does_not_allow_wt(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "_wt"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_does_not_allow_sy(self):
        self.dataframe.loc[0, [hgvs_pro_column]] = "_sy"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_parses_numeric_column_values_into_float(self):
        self.dataframe.loc[0, [required_score_column]] = "1.1"
        self.assertTrue(type(self.dataframe[required_score_column][0]) == str)
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        self.assertFalse(type(self.dataframe[required_score_column][0]) == float)
        self.dataframe.loc[0, [required_score_column]] = 1
        self.assertTrue(type(self.dataframe[required_score_column][0]) == int)
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        self.assertTrue(type(self.dataframe[required_score_column][0]) == float)

    # TODO: validate hgvs string should check this
    def test_does_not_split_double_quoted_variants(self):
        '''hgvs = "c.[123A>G;124A>G]"
        data = '{},{}\n"{}",1.0'.format(self.HGVS_NT_COL, self.SCORE_COL, hgvs)

        dataset = MaveDataset.for_scores(StringIO(data))
        dataset.validate()

        self.assertTrue(dataset.is_valid)
        self.assertIn(hgvs, dataset.data()[self.HGVS_NT_COL])

    # def test_invalid_non_double_quoted_multi_variant_row(self):
    #     hgvs = "{},{}".format(generate_hgvs(), generate_hgvs())
    #     data = "{},{}\n'{}',1.0".format(
    #         constants.hgvs_nt_column, required_score_column, hgvs
    #     )
    #     with self.assertRaises(ValidationError):
    #         _ = validate_variant_rows(BytesIO(data.encode()))'''

    def test_invalid_genomic_and_transcript_mixed_in_nt_column(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.4A>G"
        self.dataframe = self.dataframe.drop([hgvs_splice_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_nt_not_genomic_when_splice_present(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.4A>G"
        self.dataframe.loc[1, [hgvs_nt_column]] = "c.5C>G"
        self.dataframe.loc[2, [hgvs_nt_column]] = "c.6A>G"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_noncoding_hgvs_nt_should_not_have_hgvs_pro_columns(self):
        self.dataframe = self.dataframe.drop([hgvs_splice_column], axis=1)
        self.dataframe.loc[0, [hgvs_nt_column]] = "n.4A>G"
        self.dataframe.loc[1, [hgvs_nt_column]] = "n.5C>G"
        self.dataframe.loc[2, [hgvs_nt_column]] = "n.6A>G"
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        self.dataframe.loc[0, [hgvs_pro_column]] = None
        self.dataframe.loc[1, [hgvs_pro_column]] = None
        self.dataframe.loc[2, [hgvs_pro_column]] = None
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_coding_hgvs_nt_may_have_hgvs_pro_columns(self):
        self.dataframe = self.dataframe.drop([hgvs_splice_column], axis=1)
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.4A>G"
        self.dataframe.loc[1, [hgvs_nt_column]] = "c.5C>G"
        self.dataframe.loc[2, [hgvs_nt_column]] = "c.6A>G"
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        self.dataframe = self.dataframe.drop([hgvs_pro_column], axis=1)
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_splice_not_defined_when_nt_is_genomic(self):
        self.dataframe = self.dataframe.drop([hgvs_splice_column], axis=1)
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_zero_is_not_parsed_as_none(self):
        self.dataframe.loc[0, [required_score_column]] = 0.0
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        hgvs = "c.4A>G"
        data = "{},{}\n{},0.0".format(hgvs_nt_column, required_score_column, hgvs)
        df = pd.read_csv(StringIO(data), sep=",")
        validate_values_by_column(df, target_seq=self.target_seq)
        self.assertEqual(df[required_score_column].values[0], 0)

    def test_invalid_close_to_zero_is_not_parsed_as_none(self):
        self.dataframe.loc[0, [required_score_column]] = 5.6e-15
        validate_values_by_column(self.dataframe, target_seq=self.target_seq)
        hgvs = "c.4A>G"
        data = "{},{}\n{},5.6e-15".format(hgvs_nt_column, required_score_column, hgvs)
        df = pd.read_csv(StringIO(data), sep=",")
        validate_values_by_column(df, target_seq=self.target_seq)
        self.assertEqual(df[required_score_column].values[0], 5.6e-15)

    def test_mismatched_variants_and_column_names(self):
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["p.Thr2Ala", "p.Thr2Arg", "p.Thr2="],
                hgvs_pro_column: ["g.4A>G", "g.5C>G", "g.6A>G"],
                hgvs_splice_column: ["c.4A>G", "c.5C>G", "c.6A>G"],
                required_score_column: [1.000, 0.5, 1.5],
            }
        )
        with self.assertRaises(ValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)


class TestValidateIndexColumn(TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["c.1A>G", "c.2C>G", "c.3A>G"],
                hgvs_pro_column: ["p.Thr1Ala", "p.Thr1Arg", "p.="],
                required_score_column: [1.0, 0.5, 1.5],
            }
        )

    def test_valid(self):
        validate_index_column(self.dataframe["hgvs_nt"], "nt")
        self.dataframe = self.dataframe.drop([hgvs_nt_column], axis=1)
        validate_index_column(self.dataframe["hgvs_pro"], "pro")

    def test_invalid_same_hgvs_nt_defined_in_two_rows(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.2C>G"
        with self.assertRaises(ValidationError):
            validate_index_column(self.dataframe["hgvs_nt"], "nt")

    def test_invalid_same_variant_defined_in_two_rows_in_hgvs_pro_when_pro_is_primary_column(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column], axis=1)
        self.dataframe.loc[0, [hgvs_pro_column]] = "p.Thr1Arg"
        with self.assertRaises(ValidationError):
            validate_index_column(self.dataframe["hgvs_pro"], "pro")

    def test_error_missing_value_in_nt_column_when_nt_is_primary(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = np.nan
        with self.assertRaises(ValidationError):
            validate_index_column(self.dataframe["hgvs_nt"], "nt")

    def test_error_missing_value_in_pro_column_when_pro_is_primary(self):
        self.dataframe = self.dataframe.drop([hgvs_nt_column], axis=1)
        self.dataframe.loc[0, [hgvs_pro_column]] = np.nan
        with self.assertRaises(ValidationError):
            validate_index_column(self.dataframe["hgvs_pro"], "pro")


class TestValidateScore(TestCase):
    def test_valid_score(self):
        validate_score(1.1)

    def test_invalid_score(self):
        with self.assertRaises(ValidationError):
            validate_score("a")


class TestHgvsColumnsDefineSameVariants(TestCase):
    def setUp(self):
        self.target_seq = "ATGACA"
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["g.4A>G", "g.5C>G", "g.6A>G"],
                hgvs_pro_column: ["p.Thr2Ala", "p.Thr2Arg", "p.Thr2="],
                required_score_column: [1.000, 0.5, 1.5],
            }
        )

    def test_valid(self):
        for i in range(3):
            validate_hgvs_nt_and_hgvs_pro_represent_same_change(target_seq=self.target_seq,
                                                                nt=self.dataframe[hgvs_nt_column][i],
                                                                pro=self.dataframe[hgvs_pro_column][i],
                                                                row=i)

    def test_invalid_nt_and_pro_do_not_represent_same_change(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "g.2C>G"
        with self.assertRaises(ValidationError):
            for i in range(3):
                validate_hgvs_nt_and_hgvs_pro_represent_same_change(target_seq=self.target_seq,
                                                                    nt=self.dataframe[hgvs_nt_column][i],
                                                                    pro=self.dataframe[hgvs_pro_column][i],
                                                                    row=i)


class TestDataframesDefineSameVariants(TestCase):
    def setUp(self):
        self.scores = pd.DataFrame(
            {
                hgvs_nt_column: ["c.1A>G"],
                hgvs_pro_column: ["p.Leu5Glu"],
                hgvs_splice_column: ["c.1A>G"],
            }
        )
        self.counts = pd.DataFrame(
            {
                hgvs_nt_column: ["c.1A>G"],
                hgvs_pro_column: ["p.Leu5Glu"],
                hgvs_splice_column: ["c.1A>G"],
            }
        )

    def test_valid(self):
        validate_dataframes_define_same_variants(self.scores, self.counts)

    def test_counts_defines_different_nt_variants(self):
        self.counts[hgvs_nt_column][0] = "c.2A>G"
        with self.assertRaises(ValidationError):
            validate_dataframes_define_same_variants(self.scores, self.counts)

    def test_counts_defines_different_splice_variants(self):
        self.counts[hgvs_splice_column][0] = "c.2A>G"
        with self.assertRaises(ValidationError):
            validate_dataframes_define_same_variants(self.scores, self.counts)

    def test_counts_defines_different_pro_variants(self):
        self.counts[hgvs_pro_column][0] = "p.Leu75Glu"
        with self.assertRaises(ValidationError):
            validate_dataframes_define_same_variants(self.scores, self.counts)
