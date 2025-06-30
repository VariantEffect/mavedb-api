import itertools
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    guide_sequence_column,
    required_score_column,
)
from mavedb.lib.validation.dataframe.dataframe import (
    choose_dataframe_index_column,
    sort_dataframe_columns,
    standardize_dataframe,
    validate_and_standardize_dataframe_pair,
    validate_column_names,
    validate_hgvs_prefix_combinations,
    validate_no_null_rows,
    validate_variant_columns_match,
)
from mavedb.lib.validation.exceptions import ValidationError
from tests.validation.dataframe.conftest import DfTestCase


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
                    "mixed_types",
                    guide_sequence_column,
                    "null_col",
                    "\"mixed_quotes'",
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
                    "mixed_types",
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
                    "mixed_types",
                ]
            ],
            sorted_df,
        )


class TestStandardizeDataframe(DfTestCase):
    def test_preserve_standardized(self):
        standardized_df = standardize_dataframe(self.dataframe)
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_changes_case_variants(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}))
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_changes_case_scores(self):
        standardized_df = standardize_dataframe(
            self.dataframe.rename(columns={required_score_column: required_score_column.title()})
        )
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_preserves_extras_case(self):
        standardized_df = standardize_dataframe(self.dataframe.rename(columns={"extra": "extra".upper()}))
        pd.testing.assert_frame_equal(self.dataframe.rename(columns={"extra": "extra".upper()}), standardized_df)

    def test_standardize_removes_quotes(self):
        standardized_df = standardize_dataframe(
            self.dataframe.rename(columns={"extra": "'extra'", "extra2": '"extra2"'})
        )
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_removes_whitespace(self):
        standardized_df = standardize_dataframe(
            self.dataframe.rename(columns={"extra": " extra ", "extra2": "    extra2"})
        )
        pd.testing.assert_frame_equal(self.dataframe, standardized_df)

    def test_standardize_sorts_columns(self):
        standardized_df = standardize_dataframe(
            self.dataframe.loc[
                :,
                [
                    hgvs_splice_column,
                    "count2",
                    hgvs_pro_column,
                    required_score_column,
                    hgvs_nt_column,
                    "count1",
                    "extra",
                ],
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


class TestValidateStandardizeDataFramePair(DfTestCase):
    def test_no_targets(self):
        with self.assertRaises(ValueError):
            validate_and_standardize_dataframe_pair(
                self.dataframe, counts_df=None, targets=[], hdp=self.mocked_nt_human_data_provider
            )

    # TODO: Add additional DataFrames. Realistically, if other unit tests pass this function is ok


class TestNullRows(DfTestCase):
    def test_null_row(self):
        self.dataframe.iloc[1, :] = None
        with self.assertRaises(ValidationError):
            validate_no_null_rows(self.dataframe)

    def test_valid(self):
        validate_no_null_rows(self.dataframe)

    def test_only_hgvs_row(self):
        self.dataframe.loc[1, [required_score_column, "extra", "count1", "count2"]] = None
        validate_no_null_rows(self.dataframe)


class TestColumnNames(DfTestCase):
    def test_only_two_kinds_of_dataframe(self):
        with self.assertRaises(ValueError):
            validate_column_names(self.dataframe, kind="score2", is_base_editor=False)

    def test_score_df_has_score_column(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1), kind="scores", is_base_editor=False
            )

    def test_count_df_lacks_score_column(self):
        validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="counts", is_base_editor=False)
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, kind="counts", is_base_editor=False)

    def test_count_df_has_score_column(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, kind="counts", is_base_editor=False)

    def test_df_with_only_scores(self):
        validate_column_names(
            self.dataframe[[hgvs_pro_column, required_score_column]], kind="scores", is_base_editor=False
        )

    def test_count_df_must_have_data(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe[[hgvs_nt_column, hgvs_pro_column]], kind="counts", is_base_editor=False
            )

    def test_just_hgvs_nt(self):
        validate_column_names(
            self.dataframe.drop([hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores", is_base_editor=False
        )
        validate_column_names(
            self.dataframe.drop([hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1),
            kind="counts",
            is_base_editor=False,
        )

    def test_just_hgvs_pro(self):
        validate_column_names(
            self.dataframe.drop([hgvs_nt_column, hgvs_splice_column], axis=1), kind="scores", is_base_editor=False
        )
        validate_column_names(
            self.dataframe.drop([hgvs_nt_column, hgvs_splice_column, required_score_column], axis=1),
            kind="counts",
            is_base_editor=False,
        )

    def test_just_hgvs_pro_and_nt(self):
        validate_column_names(self.dataframe.drop([hgvs_splice_column], axis=1), kind="scores", is_base_editor=False)
        validate_column_names(
            self.dataframe.drop([hgvs_splice_column, required_score_column], axis=1),
            kind="counts",
            is_base_editor=False,
        )

    def test_hgvs_splice_must_have_pro_and_nt_both_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1), kind="scores", is_base_editor=False
            )

    def test_hgvs_splice_must_have_pro_and_nt_nt_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column], axis=1), kind="scores", is_base_editor=False)

    def test_hgvs_splice_must_have_pro_and_nt_pro_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column], axis=1), kind="scores", is_base_editor=False)

    def test_base_editor_must_have_nt_nt_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column], axis=1),
                kind="scores",
                is_base_editor=False,
            )

    def test_hgvs_splice_must_have_pro_and_nt_and_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, required_score_column], axis=1),
                kind="counts",
                is_base_editor=False,
            )

    def test_hgvs_splice_must_have_pro_and_nt_nt_scores_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, required_score_column], axis=1),
                kind="counts",
                is_base_editor=False,
            )

    def test_hgvs_splice_must_have_pro_and_nt_pro_scores_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_pro_column, required_score_column], axis=1),
                kind="counts",
                is_base_editor=False,
            )

    def test_no_hgvs_column_scores(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column], axis=1),
                kind="scores",
                is_base_editor=False,
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)

    def test_no_hgvs_column_counts(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop(
                    [hgvs_nt_column, hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1
                ),
                kind="counts",
                is_base_editor=False,
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)

    def test_validation_ignores_column_ordering_scores(self):
        validate_column_names(
            self.dataframe[[hgvs_nt_column, required_score_column, hgvs_pro_column, hgvs_splice_column]],
            kind="scores",
            is_base_editor=False,
        )
        validate_column_names(
            self.dataframe[[required_score_column, hgvs_nt_column, hgvs_pro_column]],
            kind="scores",
            is_base_editor=False,
        )
        validate_column_names(
            self.dataframe[[hgvs_pro_column, required_score_column, hgvs_nt_column]],
            kind="scores",
            is_base_editor=False,
        )

    def test_validation_ignores_column_ordering_counts(self):
        validate_column_names(
            self.dataframe[[hgvs_nt_column, "count1", hgvs_pro_column, hgvs_splice_column, "count2"]],
            kind="counts",
            is_base_editor=False,
        )
        validate_column_names(
            self.dataframe[["count1", "count2", hgvs_nt_column, hgvs_pro_column]], kind="counts", is_base_editor=False
        )
        validate_column_names(
            self.dataframe[[hgvs_pro_column, "count1", "count2", hgvs_nt_column]], kind="counts", is_base_editor=False
        )

    def test_validation_is_case_insensitive(self):
        validate_column_names(
            self.dataframe.rename(columns={hgvs_nt_column: hgvs_nt_column.upper()}), kind="scores", is_base_editor=False
        )
        validate_column_names(
            self.dataframe.rename(columns={required_score_column: required_score_column.title()}),
            kind="scores",
            is_base_editor=False,
        )

    def test_duplicate_hgvs_column_names_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.rename(columns={hgvs_pro_column: hgvs_nt_column}), kind="scores", is_base_editor=False
            )

    def test_duplicate_hgvs_column_names_counts(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={hgvs_pro_column: hgvs_nt_column}),
                kind="counts",
                is_base_editor=False,
            )

    def test_duplicate_score_column_names(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.rename(columns={"extra": required_score_column}), kind="scores", is_base_editor=False
            )

    def test_duplicate_data_column_names_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.rename(columns={"count2": "count1"}), kind="scores", is_base_editor=False
            )

    def test_duplicate_data_column_names_counts(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={"count2": "count1"}),
                kind="counts",
                is_base_editor=False,
            )

    # Written without @pytest.mark.parametrize. See: https://pytest.org/en/7.4.x/how-to/unittest.html#pytest-features-in-unittest-testcase-subclasses
    def test_invalid_column_names_scores(self):
        invalid_values = [None, np.nan, "", " "]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    validate_column_names(
                        self.dataframe.rename(columns={hgvs_splice_column: value}), kind="scores", is_base_editor=False
                    )

    def test_invalid_column_names_counts(self):
        invalid_values = [None, np.nan, "", " "]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    validate_column_names(
                        self.dataframe.drop([required_score_column], axis=1).rename(
                            columns={hgvs_splice_column: value}
                        ),
                        kind="counts",
                        is_base_editor=False,
                    )

    def test_ignore_column_ordering_scores(self):
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, "score", hgvs_nt_column, "count2"]],
            kind="scores",
            is_base_editor=False,
        )

    def test_ignore_column_ordering_counts(self):
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, hgvs_nt_column, "count2"]],
            kind="counts",
            is_base_editor=False,
        )

    def test_is_base_editor_and_contains_guide_sequence_column(self):
        validate_column_names(self.dataframe, kind="scores", is_base_editor=True)

    def test_is_base_editor_and_does_not_contain_guide_sequence_column(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop(guide_sequence_column, axis=1), kind="scores", is_base_editor=True
            )


class TestChooseDataframeIndexColumn(DfTestCase):
    def setUp(self):
        super().setUp()

    def test_guide_sequence_index_column(self):
        index = choose_dataframe_index_column(self.dataframe, is_base_editor=True)
        assert index == guide_sequence_column

    def test_nt_index_column(self):
        index = choose_dataframe_index_column(self.dataframe, is_base_editor=False)
        assert index == hgvs_nt_column

    def test_pro_index_column(self):
        index = choose_dataframe_index_column(self.dataframe.drop(hgvs_nt_column, axis=1), is_base_editor=False)
        assert index == hgvs_pro_column

    def test_no_valid_index_column(self):
        with self.assertRaises(ValidationError):
            choose_dataframe_index_column(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1),
                is_base_editor=False,
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
            for t in itertools.product(("c", "n", "g", "m", "o", None), ("c", "n", None), ("p", None))
            if t not in self.valid_combinations
        ]

    def test_valid_combinations(self):
        for t in self.valid_combinations:
            with self.subTest(t=t):
                validate_hgvs_prefix_combinations(*t, True)

    def test_invalid_combinations(self):
        for t in self.invalid_combinations:
            with self.subTest(t=t):
                with self.assertRaises(ValidationError):
                    validate_hgvs_prefix_combinations(*t, True)

    # TODO: biocommons.HGVS validation clashes here w/ our custom validators:
    #       n. prefix is the problematic one, for now.
    @pytest.mark.skip()
    def test_invalid_combinations_biocommons(self):
        for t in self.invalid_combinations:
            with self.subTest(t=t):
                with self.assertRaises(ValidationError):
                    validate_hgvs_prefix_combinations(*t, False)

    def test_invalid_combinations_value_error_nt(self):
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("p", None, None, True)

    def test_invalid_combinations_value_error_nt_pro(self):
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("c", None, "P", True)

    def test_invalid_combinations_value_error_splice(self):
        with self.assertRaises(ValueError):
            validate_hgvs_prefix_combinations("x", "c", "p", True)


class TestValidateVariantColumnsMatch(DfTestCase):
    def test_same_df(self):
        validate_variant_columns_match(self.dataframe, self.dataframe)

    def test_ignore_order(self):
        validate_variant_columns_match(self.dataframe, self.dataframe.iloc[::-1])

    def test_missing_column_nt(self):
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, self.dataframe.drop(hgvs_nt_column, axis=1))
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe.drop(hgvs_nt_column, axis=1), self.dataframe)

    def test_missing_column_pro(self):
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, self.dataframe.drop(hgvs_pro_column, axis=1))
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe.drop(hgvs_pro_column, axis=1), self.dataframe)

    def test_missing_column_splice(self):
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, self.dataframe.drop(hgvs_splice_column, axis=1))
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe.drop(hgvs_splice_column, axis=1), self.dataframe)

    def test_missing_column_guide(self):
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, self.dataframe.drop(guide_sequence_column, axis=1))
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe.drop(guide_sequence_column, axis=1), self.dataframe)

    def test_missing_variant_nt(self):
        df2 = self.dataframe.copy()
        df2.loc[0, hgvs_nt_column] = None
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, df2)

    def test_missing_variant_pro(self):
        df2 = self.dataframe.copy()
        df2.loc[0, hgvs_pro_column] = None
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, df2)

    def test_missing_variant_splice(self):
        df2 = self.dataframe.copy()
        df2.loc[0, hgvs_splice_column] = None
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, df2)

    def test_missing_guide(self):
        df2 = self.dataframe.copy()
        df2.loc[0, guide_sequence_column] = None
        with self.assertRaises(ValidationError):
            validate_variant_columns_match(self.dataframe, df2)
