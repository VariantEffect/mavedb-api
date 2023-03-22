from unittest import TestCase
import numpy as np
import pandas as pd
from io import StringIO

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column,
)

from mavedb.lib.validation.dataframe import (
    infer_column_type,
    validate_no_null_data_columns,
    validate_no_null_rows,
    validate_column_names,
    validate_hgvs_columns,
    validate_data_columns,
    validate_variant_column_agreement,
    sort_dataframe_columns,
    standardize_dataframe,
    DataframeValidationError,
)


class TestInferColumnType(TestCase):
    def test_floats(self):
        test_data = pd.Series([12.0, 1.0, -0.012, 5.75])
        self.assertEqual(infer_column_type(test_data), "numeric")

    def test_ints(self):
        test_data = pd.Series([12, 1, 0, -5])
        self.assertEqual(infer_column_type(test_data), "numeric")

    def test_floats_with_na(self):
        test_data = pd.Series([12.0, 1.0, None, -0.012, 5.75])
        self.assertEqual(infer_column_type(test_data), "numeric")

    def test_ints_with_na(self):
        test_data = pd.Series([12, 1, None, 0, -5])
        self.assertEqual(infer_column_type(test_data), "numeric")

    def test_convertable_strings(self):
        test_data = pd.Series(["12.5", 1.25, "0", "-5"])
        self.assertEqual(infer_column_type(test_data), "numeric")

    def test_strings(self):
        test_data = pd.Series(["hello", "test", "suite", "123abc"])
        self.assertEqual(infer_column_type(test_data), "string")

    def test_strings_with_na(self):
        test_data = pd.Series(["hello", "test", None, "suite", "123abc"])
        self.assertEqual(infer_column_type(test_data), "string")

    def test_mixed(self):
        test_data = pd.Series(["hello", 12.123, -75, "123abc"])
        self.assertEqual(infer_column_type(test_data), "mixed")

    def test_mixed_with_na(self):
        test_data = pd.Series(["hello", None, 12.123, -75, "123abc"])
        self.assertEqual(infer_column_type(test_data), "mixed")

    def test_all_na(self):
        test_data = pd.Series([None] * 5)
        self.assertEqual(infer_column_type(test_data), "empty")


class DfTestCase(TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
                required_score_column: [1.0, 2.0],
                "extra": [12.0, 3.0],
                "count1": [3.0, 5.0],
                "count2": [9, 10],
            })

        self.target_seq = "ATG"


class TestNullDataColumns(DfTestCase):
    def test_valid(self):
        validate_no_null_data_columns(self.dataframe)

    def test_null_score_column(self):
        self.dataframe[required_score_column] = None
        with self.assertRaises(DataframeValidationError):
            validate_no_null_data_columns(self.dataframe)

    def test_null_extra_column(self):
        self.dataframe["extra"] = None
        with self.assertRaises(DataframeValidationError):
            validate_no_null_data_columns(self.dataframe)

    def test_allow_null_hgvs_columns(self):
        self.dataframe[hgvs_splice_column] = None
        validate_no_null_data_columns(self.dataframe)
        self.dataframe[hgvs_nt_column] = None
        validate_no_null_data_columns(self.dataframe)

    def test_allow_missing_scores(self):
        self.dataframe.loc[0, required_score_column] = None
        validate_no_null_data_columns(self.dataframe)

    def test_allow_missing_extras(self):
        self.dataframe.loc[1, "extra"] = None
        validate_no_null_data_columns(self.dataframe)


class TestNullRows(DfTestCase):
    def test_valid(self):
        validate_no_null_rows(self.dataframe)

    def test_null_row(self):
        self.dataframe.loc[1] = None
        with self.assertRaises(DataframeValidationError):
            validate_no_null_rows(self.dataframe)

    def test_only_hgvs_row(self):
        self.dataframe.loc[1, [required_score_column, "extra", "count1", "count2"]] = None
        validate_no_null_rows(self.dataframe)


class TestColumnNames(DfTestCase):
    def test_only_two_kinds_of_dataframe(self):
        with self.assertRaises(ValueError):
            validate_column_names(self.dataframe, kind="score2")

    def test_require_dataframe_kind(self):
        with self.assertRaises(TypeError):
            validate_column_names(self.dataframe)

    def test_score_df_has_score_column(self):
        validate_column_names(self.dataframe, kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="scores")

    def test_count_df_lacks_score_column(self):
        validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="counts")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe, kind="counts")

    def test_df_with_only_scores(self):
        validate_column_names(self.dataframe[[hgvs_pro_column, required_score_column]], kind="scores")

    def test_count_df_must_have_data(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe[[hgvs_nt_column, hgvs_pro_column]], kind="counts")

    def test_just_hgvs_nt(self):
        validate_column_names(self.dataframe.drop([hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(self.dataframe.drop([hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1), kind="counts")

    def test_just_hgvs_pro(self):
        validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_splice_column, required_score_column], axis=1), kind="counts")

    def test_just_hgvs_pro_and_nt(self):
        validate_column_names(self.dataframe.drop([hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(self.dataframe.drop([hgvs_splice_column, required_score_column], axis=1), kind="counts")

    def test_hgvs_splice_must_have_pro_and_nt(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column], axis=1), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column], axis=1), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, required_score_column], axis=1), kind="counts")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, required_score_column], axis=1), kind="counts")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column, required_score_column], axis=1), kind="counts")

    def test_no_hgvs_column(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1), kind="counts")

    def test_validation_ignores_column_ordering(self):
        validate_column_names(self.dataframe[[hgvs_nt_column, required_score_column, hgvs_pro_column, hgvs_splice_column]], kind="scores")
        validate_column_names(self.dataframe[[required_score_column, hgvs_nt_column, hgvs_pro_column]], kind="scores")
        validate_column_names(self.dataframe[[hgvs_pro_column, required_score_column, hgvs_nt_column]], kind="scores")

        validate_column_names(self.dataframe[[hgvs_nt_column, "count1", hgvs_pro_column, hgvs_splice_column, "count2"]], kind="counts")
        validate_column_names(self.dataframe[["count1", "count2", hgvs_nt_column, hgvs_pro_column]], kind="counts")
        validate_column_names(self.dataframe[[hgvs_pro_column, "count1", "count2", hgvs_nt_column]], kind="counts")

    def test_validation_is_case_insensitive(self):
        validate_column_names(self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}), kind="scores")
        validate_column_names(self.dataframe.rename(columns={required_score_column: required_score_column.title()}), kind="scores")

    def test_duplicate_hgvs_column_names(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.rename(columns={hgvs_pro_column: hgvs_nt_column}), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([required_score_column], axis=1).rename(columns={hgvs_pro_column: hgvs_nt_column}), kind="counts")

    def test_duplicate_score_column_names(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.rename(columns={"extra": required_score_column}), kind="scores")

    def test_duplicate_data_column_names(self):
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.rename(columns={"count2": "count1"}), kind="scores")
        with self.assertRaises(DataframeValidationError):
            validate_column_names(self.dataframe.drop([required_score_column], axis=1).rename(columns={"count2": "count1"}), kind="counts")

    def test_invalid_column_names(self):
        invalid_values = [None, np.nan, "", " "]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(DataframeValidationError):
                    validate_column_names(self.dataframe.rename(columns={hgvs_splice_column: value}), kind="scores")
                with self.assertRaises(DataframeValidationError):
                    validate_column_names(self.dataframe.drop([required_score_column], axis=1).rename(columns={hgvs_splice_column: value}), kind="counts")

    def test_ignore_column_ordering(self):
        validate_column_names(self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, "score", hgvs_nt_column, "count2"]], kind="scores")
        validate_column_names(self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, hgvs_nt_column, "count2"]], kind="counts")


class TestSortDataframeColumns(DfTestCase):
    def test_preserve_sorted(self):
        sorted_df = sort_dataframe_columns(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe(self):
        sorted_df = sort_dataframe_columns(self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, required_score_column, hgvs_nt_column, "count2"]])
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe_is_case_insensitive(self):
        self.dataframe = self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()})
        sorted_df = sort_dataframe_columns(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe_preserves_extras_order(self):
        sorted_df = sort_dataframe_columns(self.dataframe[[hgvs_splice_column, "count2", hgvs_pro_column, required_score_column, hgvs_nt_column, "count1", "extra"]])
        pd.testing.assert_frame_equal(self.dataframe[[hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, required_score_column, "count2", "count1", "extra"]], sorted_df)


class TestStandardizeDataframe(DfTestCase):
    def test_preserve_standardized(self):
        standardized_df = standardize_dataframe(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_changes_case(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}))
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={required_score_column: required_score_column.title()}))
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_preserves_extras_case(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={"extra": "extra".upper()}))
        pd.testing.assert_frame_equal(self.dataframe.rename(columns={"extra": "extra".upper()}), standardized_df)

    def test_standardize_sorts_columns(self):
        standardized_df = standardize_dataframe(self.dataframe[[hgvs_splice_column, "count2", hgvs_pro_column, required_score_column, hgvs_nt_column, "count1", "extra"]])
        pd.testing.assert_frame_equal(self.dataframe[[hgvs_nt_column, hgvs_splice_column, hgvs_pro_column, required_score_column, "count2", "count1", "extra"]], standardized_df)


class TestValidateHgvsColumns(DfTestCase):
    def setUp(self):
        super().setUp()

        self.valid_hgvs_dataframes = [
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["m.1A>G", "m.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["n.1A>G", "n.1A>T"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["n.1A>G", "n.1A>T"],
            }),
        ]

        self.invalid_hgvs_dataframes = [
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["n.1A>G", "n.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["n.1A>G", "n.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["c.1A>G", "c.1A>T"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["r.1a>g", "r.1a>u"],
            }),
            pd.DataFrame({
                hgvs_nt_column: ["r.1a>g", "r.1a>u"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            }),
        ]

    def test_valid_hgvs_combinations(self):
        for df in self.valid_hgvs_dataframes:
            with self.subTest(df=df):
                validate_hgvs_columns(df, target_seq=self.target_seq)

    def test_invalid_hgvs_combinations(self):
        for df in self.invalid_hgvs_dataframes:
            with self.subTest(df=df):
                with self.assertRaises(DataframeValidationError):
                    validate_hgvs_columns(df, target_seq=self.target_seq)

    def test_hgvs_is_not_a_string(self):
        self.dataframe.loc[0, hgvs_nt_column] = 1.0
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_protein_hgvs_in_nt(self):
        self.dataframe.loc[0, hgvs_nt_column] = self.dataframe.loc[0, hgvs_pro_column]
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_inconsistent_hgvs_nt(self):
        self.dataframe.loc[0, hgvs_nt_column] = "g.1A>T"
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_protein_hgvs_in_splice(self):
        self.dataframe.loc[0, hgvs_splice_column] = self.dataframe.loc[0, hgvs_pro_column]
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_invalid_hgvs_pro_in_column(self):
        self.dataframe.loc[0, hgvs_pro_column] = "c.1A>G"
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_invalid_hgvs_splice_in_column(self):
        self.dataframe.loc[0, hgvs_splice_column] = "g.1A>G"
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_does_not_allow_wt(self):
        self.dataframe.loc[0, hgvs_nt_column] = "_wt"
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)

    def test_does_not_allow_sy(self):
        self.dataframe.loc[0, hgvs_nt_column] = "_sy"
        with self.assertRaises(DataframeValidationError):
            validate_hgvs_columns(self.dataframe, target_seq=self.target_seq)


class TestValidateDataColumns(DfTestCase):
    def test_valid(self):
        validate_data_columns(self.dataframe)

    def test_non_numeric_values_in_score_column(self):
        self.dataframe.loc[0, required_score_column] = "not a float"
        with self.assertRaises(DataframeValidationError):
            validate_data_columns(self.dataframe)

    def test_parses_numeric_column_values_into_float(self):
        self.dataframe.loc[0, [required_score_column]] = "1.1"
        self.assertTrue(type(self.dataframe[required_score_column][0]) == str)
        with self.assertRaises(DataframeValidationError):
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
        with self.assertRaises(DataframeValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_invalid_nt_not_genomic_when_splice_present(self):
        self.dataframe.loc[0, [hgvs_nt_column]] = "c.4A>G"
        self.dataframe.loc[1, [hgvs_nt_column]] = "c.5C>G"
        self.dataframe.loc[2, [hgvs_nt_column]] = "c.6A>G"
        with self.assertRaises(DataframeValidationError):
            validate_values_by_column(self.dataframe, target_seq=self.target_seq)

    def test_noncoding_hgvs_nt_should_not_have_hgvs_pro_columns(self):
        self.dataframe = self.dataframe.drop([hgvs_splice_column], axis=1)
        self.dataframe.loc[0, [hgvs_nt_column]] = "n.4A>G"
        self.dataframe.loc[1, [hgvs_nt_column]] = "n.5C>G"
        self.dataframe.loc[2, [hgvs_nt_column]] = "n.6A>G"
        with self.assertRaises(DataframeValidationError):
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
        with self.assertRaises(DataframeValidationError):
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
        with self.assertRaises(DataframeValidationError):
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
        validate_variant_column_agreement(self.scores, self.counts)

    def test_counts_defines_different_nt_variants(self):
        self.counts[hgvs_nt_column][0] = "c.2A>G"
        with self.assertRaises(ValidationError):
            validate_variant_column_agreement(self.scores, self.counts)

    def test_counts_defines_different_splice_variants(self):
        self.counts[hgvs_splice_column][0] = "c.2A>G"
        with self.assertRaises(ValidationError):
            validate_variant_column_agreement(self.scores, self.counts)

    def test_counts_defines_different_pro_variants(self):
        self.counts[hgvs_pro_column][0] = "p.Leu75Glu"
        with self.assertRaises(ValidationError):
            validate_variant_column_agreement(self.scores, self.counts)
