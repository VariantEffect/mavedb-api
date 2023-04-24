from unittest import TestCase
import numpy as np
import pandas as pd
import itertools
import pytest

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column,
)

from mavedb.lib.validation.dataframe import (
    infer_column_type,
    sort_dataframe_columns,
    standardize_dataframe,
    validate_no_null_rows,
    validate_column_names,
    validate_hgvs_column,
    validate_hgvs_prefix_combinations,
    validate_data_column,
    validate_variant_columns_match,
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
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
                required_score_column: [1.0, 2.0],
                "extra": [12.0, 3.0],
                "count1": [3.0, 5.0],
                "count2": [9, 10],
                "extra2": ["pathogenic", "benign"],
            }
        )

        self.target_seq = "ATG"
        self.target_seq_type = "dna"


class TestValidateDataColumn(DfTestCase):
    def test_valid(self):
        validate_data_column(self.dataframe[required_score_column])

    def test_null_column(self):
        self.dataframe[required_score_column] = None
        with self.assertRaises(ValidationError):
            validate_data_column(self.dataframe[required_score_column])

    def test_missing_data(self):
        self.dataframe.loc[0, "extra"] = None
        validate_data_column(self.dataframe["extra"])

    def test_force_numeric(self):
        with self.assertRaises(ValidationError):
            validate_data_column(self.dataframe["extra2"], force_numeric=True)

    def test_mixed_types_invalid(self):
        with self.assertRaises(ValidationError):
            validate_data_column(pd.Series(["test", 1.0]))


class TestNullRows(DfTestCase):
    def test_valid(self):
        validate_no_null_rows(self.dataframe)

    def test_null_row(self):
        self.dataframe.loc[1] = None
        with self.assertRaises(ValidationError):
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
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="scores")

    def test_count_df_lacks_score_column(self):
        validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="counts")
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, kind="counts")

    def test_df_with_only_scores(self):
        validate_column_names(self.dataframe[[hgvs_pro_column, required_score_column]], kind="scores")

    def test_count_df_must_have_data(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe[[hgvs_nt_column, hgvs_pro_column]], kind="counts")

    def test_just_hgvs_nt(self):
        validate_column_names(self.dataframe.drop([hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(
            self.dataframe.drop([hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1), kind="counts"
        )

    def test_just_hgvs_pro(self):
        validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(
            self.dataframe.drop([hgvs_nt_column, hgvs_splice_column, required_score_column], axis=1), kind="counts"
        )

    def test_just_hgvs_pro_and_nt(self):
        validate_column_names(self.dataframe.drop([hgvs_splice_column], axis=1), kind="scores")
        validate_column_names(self.dataframe.drop([hgvs_splice_column, required_score_column], axis=1), kind="counts")

    def test_hgvs_splice_must_have_pro_and_nt(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1), kind="scores")
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column], axis=1), kind="scores")
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column], axis=1), kind="scores")
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, required_score_column], axis=1), kind="counts"
            )
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, required_score_column], axis=1), kind="counts")
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column, required_score_column], axis=1), kind="counts")

    def test_no_hgvs_column(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores"
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop(
                    [hgvs_nt_column, hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1
                ),
                kind="counts",
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)

    def test_validation_ignores_column_ordering(self):
        validate_column_names(
            self.dataframe[[hgvs_nt_column, required_score_column, hgvs_pro_column, hgvs_splice_column]], kind="scores"
        )
        validate_column_names(self.dataframe[[required_score_column, hgvs_nt_column, hgvs_pro_column]], kind="scores")
        validate_column_names(self.dataframe[[hgvs_pro_column, required_score_column, hgvs_nt_column]], kind="scores")

        validate_column_names(
            self.dataframe[[hgvs_nt_column, "count1", hgvs_pro_column, hgvs_splice_column, "count2"]], kind="counts"
        )
        validate_column_names(self.dataframe[["count1", "count2", hgvs_nt_column, hgvs_pro_column]], kind="counts")
        validate_column_names(self.dataframe[[hgvs_pro_column, "count1", "count2", hgvs_nt_column]], kind="counts")

    def test_validation_is_case_insensitive(self):
        validate_column_names(self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}), kind="scores")
        validate_column_names(
            self.dataframe.rename(columns={required_score_column: required_score_column.title()}), kind="scores"
        )

    def test_duplicate_hgvs_column_names(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={hgvs_pro_column: hgvs_nt_column}), kind="scores")
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={hgvs_pro_column: hgvs_nt_column}),
                kind="counts",
            )

    def test_duplicate_score_column_names(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={"extra": required_score_column}), kind="scores")

    def test_duplicate_data_column_names(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={"count2": "count1"}), kind="scores")
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={"count2": "count1"}), kind="counts"
            )

    def test_invalid_column_names(self):
        invalid_values = [None, np.nan, "", " "]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    validate_column_names(self.dataframe.rename(columns={hgvs_splice_column: value}), kind="scores")
                with self.assertRaises(ValidationError):
                    validate_column_names(
                        self.dataframe.drop([required_score_column], axis=1).rename(
                            columns={hgvs_splice_column: value}
                        ),
                        kind="counts",
                    )

    def test_ignore_column_ordering(self):
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, "score", hgvs_nt_column, "count2"]],
            kind="scores",
        )
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, hgvs_nt_column, "count2"]],
            kind="counts",
        )


class TestSortDataframeColumns(DfTestCase):
    def test_preserve_sorted(self):
        sorted_df = sort_dataframe_columns(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe(self):
        sorted_df = sort_dataframe_columns(
            self.dataframe[
                [
                    hgvs_splice_column,
                    "extra",
                    "count1",
                    hgvs_pro_column,
                    required_score_column,
                    hgvs_nt_column,
                    "count2",
                    "extra2",
                ]
            ]
        )
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe_is_case_insensitive(self):
        self.dataframe = self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()})
        sorted_df = sort_dataframe_columns(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, sorted_df)

    def test_sort_dataframe_preserves_extras_order(self):
        sorted_df = sort_dataframe_columns(
            self.dataframe[
                [
                    hgvs_splice_column,
                    "count2",
                    hgvs_pro_column,
                    required_score_column,
                    hgvs_nt_column,
                    "count1",
                    "extra2",
                    "extra",
                ]
            ]
        )
        pd.testing.assert_frame_equal(
            self.dataframe[
                [
                    hgvs_nt_column,
                    hgvs_splice_column,
                    hgvs_pro_column,
                    required_score_column,
                    "count2",
                    "count1",
                    "extra2",
                    "extra",
                ]
            ],
            sorted_df,
        )


class TestStandardizeDataframe(DfTestCase):
    def test_preserve_standardized(self):
        standardized_df = standardize_dataframe(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_changes_case(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}))
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)
        standardized_df = standardize_dataframe(
            self.dataframe.rename(columns={required_score_column: required_score_column.title()})
        )
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_preserves_extras_case(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={"extra": "extra".upper()}))
        pd.testing.assert_frame_equal(self.dataframe.rename(columns={"extra": "extra".upper()}), standardized_df)

    def test_standardize_sorts_columns(self):
        standardized_df = standardize_dataframe(
            self.dataframe[
                [
                    hgvs_splice_column,
                    "count2",
                    hgvs_pro_column,
                    required_score_column,
                    hgvs_nt_column,
                    "count1",
                    "extra",
                ]
            ]
        )
        pd.testing.assert_frame_equal(
            self.dataframe[
                [
                    hgvs_nt_column,
                    hgvs_splice_column,
                    hgvs_pro_column,
                    required_score_column,
                    "count2",
                    "count1",
                    "extra",
                ]
            ],
            standardized_df,
        )


class TestValidateHgvsColumn(DfTestCase):
    def setUp(self):
        super().setUp()

        self.valid_hgvs_columns = [
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column),
            pd.Series(["m.1A>G", "m.1A>T"], name=hgvs_nt_column),
            pd.Series(["c.1A>G", "c.1A>T"], name=hgvs_nt_column),
            pd.Series(["n.1A>G", "n.1A>T"], name=hgvs_nt_column),
            pd.Series(["c.1A>G", "c.1A>T"], name=hgvs_splice_column),
            pd.Series(["p.Met1Val", "p.Met1Leu"], name=hgvs_pro_column),
        ]

        self.valid_hgvs_columns_invalid_for_index = [
            # missing data
            pd.Series(["c.1A>G", None], name=hgvs_nt_column),
            pd.Series([None, "p.Met1Val"], name=hgvs_pro_column),
            pd.Series([None, None], name=hgvs_nt_column),
            pd.Series([None, None], name=hgvs_pro_column),
            # duplicate rows
            pd.Series(["c.1A>G", "c.1A>G"], name=hgvs_nt_column),
            pd.Series(["p.Met1Val", "p.Met1Val"], name=hgvs_pro_column),
        ]

        self.invalid_hgvs_columns_by_name = [
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_splice_column),
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_pro_column),
            pd.Series(["c.1A>G", "c.1A>T"], name=hgvs_pro_column),
            pd.Series(["n.1A>G", "n.1A>T"], name=hgvs_pro_column),
            pd.Series(["p.Met1Val", "p.Met1Leu"], name=hgvs_nt_column),
        ]

        self.invalid_hgvs_columns_by_contents = [
            pd.Series(["r.1a>g", "r.1a>u"], name=hgvs_splice_column),  # rna not allowed
            pd.Series(["r.1a>g", "r.1a>u"], name=hgvs_nt_column),  # rna not allowed
            pd.Series(["c.1A>G", "c.5A>T"], name=hgvs_nt_column),  # out of bounds for target
            pd.Series(["c.1A>G", "_wt"], name=hgvs_nt_column),  # old special variant
            pd.Series(["p.Met1Leu", "_sy"], name=hgvs_pro_column),  # old special variant
            pd.Series(["n.1A>G", "c.1A>T"], name=hgvs_nt_column),  # mixed prefix
            pd.Series(["c.1A>G", "p.Met1Leu"], name=hgvs_pro_column),  # mixed types/prefix
            pd.Series(["c.1A>G", 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_splice_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_pro_column),  # contains numeric
        ]

    def test_valid_columns(self):
        for column in self.valid_hgvs_columns:
            with self.subTest(column=column):
                validate_hgvs_column(
                    column, is_index=False, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                )
        for column in self.valid_hgvs_columns_invalid_for_index:
            with self.subTest(column=column):
                validate_hgvs_column(
                    column, is_index=False, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                )

    def test_index_columns(self):
        for column in self.valid_hgvs_columns:
            with self.subTest(column=column):
                validate_hgvs_column(
                    column, is_index=True, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                )
        for column in self.valid_hgvs_columns_invalid_for_index:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_column(
                        column, is_index=True, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                    )

    def test_invalid_column_values(self):
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_column(
                        column, is_index=False, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                    )
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_column(
                        column, is_index=True, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                    )

    def test_valid_column_values_wrong_column_name(self):
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_column(
                        column, is_index=False, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                    )
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_column(
                        column, is_index=True, target_seq=self.target_seq, target_seq_type=self.target_seq_type
                    )


class TestValidateHgvsPrefixCombinations(TestCase):
    def setUp(self):
        self.valid_combinations = [
            ("g", "c", "p"),
            ("m", "c", "p"),
            ("o", "c", "p"),
            ("g", "n", None),
            ("m", "n", None),
            ("o", "n", None),
            ("n", None, None),
            ("c", None, "p"),
            (None, None, "p"),
            (None, None, None),  # valid for this validator, but a dataframe with no variants should be caught upstream
        ]
        self.invalid_combinations = [
            t
            for t in itertools.product(list("cngmo") + [None], ("c", "n", None), ("p", None))
            if t not in self.valid_combinations
        ]

    def test_valid_combinations(self):
        for t in self.valid_combinations:
            with self.subTest(t=t):
                validate_hgvs_prefix_combinations(*t)

    def test_invalid_combinations(self):
        for t in self.invalid_combinations:
            with self.subTest(t=t):
                with self.assertRaises(ValidationError):
                    validate_hgvs_prefix_combinations(*t)

    def test_invalid_combinations_value_error(self):
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("p", None, None)
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("c", None, "P")
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("x", "c", "p")


class TestValidateVariantColumnsMatch(DfTestCase):
    def test_same_df(self):
        validate_variant_columns_match(self.dataframe, self.dataframe)

    def test_ignore_order(self):
        validate_variant_columns_match(self.dataframe, self.dataframe.iloc[::-1])

    def test_missing_column(self):
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, self.dataframe.drop(hgvs_nt_column, axis=1))
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe.drop(hgvs_nt_column, axis=1), self.dataframe)

    def test_missing_variant(self):
        df2 = self.dataframe.copy()
        df2.loc[0, hgvs_pro_column] = None
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, df2)
