import itertools
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
import cdot.hgvs.dataproviders

from unittest.mock import patch
from tests.helpers.constants import VALID_ACCESSION, TEST_CDOT_TRANSCRIPT

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column,
)
from mavedb.lib.validation.dataframe import (
    choose_dataframe_index_column,
    generate_variant_prefixes,
    infer_column_type,
    sort_dataframe_columns,
    standardize_dataframe,
    validate_and_standardize_dataframe_pair,
    validate_column_names,
    validate_data_column,
    validate_hgvs_genomic_column,
    validate_hgvs_prefix_combinations,
    validate_hgvs_transgenic_column,
    validate_no_null_rows,
    validate_variant_column,
    validate_variant_columns_match,
    validate_variant_formatting,
)
from mavedb.lib.validation.exceptions import ValidationError


@pytest.fixture
def data_provider_class_attr(request, data_provider):
    """
    Sets the `human_data_provider` attribute on the class from the requesting
    test context to the `data_provider` fixture. This allows fixture use across
    the `unittest.TestCase` class.
    """
    request.cls.human_data_provider = data_provider


# Special DF Test Case that contains dummy data for tests below
@pytest.mark.usefixtures("data_provider_class_attr")
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
                "mixed_types": ["test", 1.0],
                "null_col": [None, None],
            }
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
                    "null_col",
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

    def test_standardice_changes_case_scores(self):
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


class TestValidateStandardizeDataFramePair(DfTestCase):
    def test_no_targets(self):
        with self.assertRaises(ValueError):
            validate_and_standardize_dataframe_pair(
                self.dataframe, counts_df=None, targets=[], hdp=self.human_data_provider
            )

    # TODO: Add additional DataFrames. Realistically, if other unit tests pass this function is ok


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
            validate_data_column(self.dataframe["mixed_types"])


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
            validate_column_names(self.dataframe, kind="score2")

    def test_score_df_has_score_column(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="scores")

    def test_count_df_lacks_score_column(self):
        validate_column_names(self.dataframe.drop([required_score_column], axis=1), kind="counts")
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe, kind="counts")

    def test_count_df_has_score_column(self):
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

    def test_hgvs_splice_must_have_pro_and_nt_both_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1), kind="scores")

    def test_hgvs_splice_must_have_pro_and_nt_nt_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column], axis=1), kind="scores")

    def test_hgvs_splice_must_have_pro_and_nt_pro_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column], axis=1), kind="scores")

    def test_hgvs_splice_must_have_pro_and_nt_and_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, required_score_column], axis=1), kind="counts"
            )

    def test_hgvs_splice_must_have_pro_and_nt_nt_scores_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_nt_column, required_score_column], axis=1), kind="counts")

    def test_hgvs_splice_must_have_pro_and_nt_pro_scores_absent(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.drop([hgvs_pro_column, required_score_column], axis=1), kind="counts")

    def test_no_hgvs_column_scores(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop([hgvs_nt_column, hgvs_pro_column, hgvs_splice_column], axis=1), kind="scores"
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)

    def test_no_hgvs_column_counts(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_column_names(
                self.dataframe.drop(
                    [hgvs_nt_column, hgvs_pro_column, hgvs_splice_column, required_score_column], axis=1
                ),
                kind="counts",
            )
        assert "dataframe does not define any variant columns" in str(exc_info.value)

    def test_validation_ignores_column_ordering_scores(self):
        validate_column_names(
            self.dataframe[[hgvs_nt_column, required_score_column, hgvs_pro_column, hgvs_splice_column]], kind="scores"
        )
        validate_column_names(self.dataframe[[required_score_column, hgvs_nt_column, hgvs_pro_column]], kind="scores")
        validate_column_names(self.dataframe[[hgvs_pro_column, required_score_column, hgvs_nt_column]], kind="scores")

    def test_validation_ignores_column_ordering_counts(self):
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

    def test_duplicate_hgvs_column_names_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={hgvs_pro_column: hgvs_nt_column}), kind="scores")

    def test_duplicate_hgvs_column_names_counts(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={hgvs_pro_column: hgvs_nt_column}),
                kind="counts",
            )

    def test_duplicate_score_column_names(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={"extra": required_score_column}), kind="scores")

    def test_duplicate_data_column_names_scores(self):
        with self.assertRaises(ValidationError):
            validate_column_names(self.dataframe.rename(columns={"count2": "count1"}), kind="scores")

    def test_duplicate_data_column_names_counts(self):
        with self.assertRaises(ValidationError):
            validate_column_names(
                self.dataframe.drop([required_score_column], axis=1).rename(columns={"count2": "count1"}), kind="counts"
            )

    # Written without @pytest.mark.parametrize. See: https://pytest.org/en/7.4.x/how-to/unittest.html#pytest-features-in-unittest-testcase-subclasses
    def test_invalid_column_names_scores(self):
        invalid_values = [None, np.nan, "", " "]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    validate_column_names(self.dataframe.rename(columns={hgvs_splice_column: value}), kind="scores")

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
                    )

    def test_ignore_column_ordering_scores(self):
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, "score", hgvs_nt_column, "count2"]],
            kind="scores",
        )

    def test_ignore_column_ordering_counts(self):
        validate_column_names(
            self.dataframe[[hgvs_splice_column, "extra", "count1", hgvs_pro_column, hgvs_nt_column, "count2"]],
            kind="counts",
        )


class TestChooseDataframeIndexColumn(DfTestCase):
    def setUp(self):
        super().setUp()

    def test_nt_index_column(self):
        index = choose_dataframe_index_column(self.dataframe)
        assert index == hgvs_nt_column

    def test_pro_index_column(self):
        index = choose_dataframe_index_column(self.dataframe.drop(hgvs_nt_column, axis=1))
        assert index == hgvs_pro_column

    def test_no_valid_index_column(self):
        with self.assertRaises(ValidationError):
            choose_dataframe_index_column(self.dataframe.drop([hgvs_nt_column, hgvs_pro_column], axis=1))


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


class TestValidateVariantFormatting(TestCase):
    def setUp(self) -> None:
        super().setUp()

        self.valid = pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column)
        self.inconsistent = pd.Series(["g.1A>G", "c.1A>T"], name=hgvs_nt_column)
        self.valid_prefixes = ["g."]
        self.invalid_prefixes = ["c."]
        self.valid_target = ["single_target"]

        self.valid_multi = pd.Series(["test1:g.1A>G", "test2:g.1A>T"], name=hgvs_nt_column)
        self.invalid_multi = pd.Series(["test3:g.1A>G", "test3:g.1A>T"], name=hgvs_nt_column)
        self.inconsistent_multi = pd.Series(["test1:g.1A>G", "test2:c.1A>T"], name=hgvs_nt_column)
        self.valid_targets = ["test1", "test2"]

    def test_single_target_valid_variants(self):
        validate_variant_formatting(self.valid, self.valid_prefixes, self.valid_target, False)

    def test_single_target_inconsistent_variants(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.inconsistent, self.valid_prefixes, self.valid_target, False)

    def test_single_target_invalid_prefixes(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.valid, self.invalid_prefixes, self.valid_target, False)

    def test_multi_target_valid_variants(self):
        validate_variant_formatting(self.valid_multi, self.valid_prefixes, self.valid_targets, True)

    def test_multi_target_inconsistent_variants(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.inconsistent_multi, self.valid_prefixes, self.valid_targets, True)

    def test_multi_target_invalid_prefixes(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.valid_multi, self.invalid_prefixes, self.valid_targets, True)

    def test_multi_target_lacking_full_coords(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.valid, self.valid_prefixes, self.valid_targets, True)

    def test_multi_target_invalid_accessions(self):
        with self.assertRaises(ValidationError):
            validate_variant_formatting(self.invalid_multi, self.valid_prefixes, self.valid_targets, True)


class TestGenerateVariantPrefixes(DfTestCase):
    def setUp(self):
        super().setUp()

        self.nt_prefixes = ["c.", "n.", "g.", "m.", "o."]
        self.splice_prefixes = ["c.", "n."]
        self.pro_prefixes = ["p."]

    def test_nt_prefixes(self):
        prefixes = generate_variant_prefixes(self.dataframe[hgvs_nt_column])
        assert prefixes == self.nt_prefixes

    def test_pro_prefixes(self):
        prefixes = generate_variant_prefixes(self.dataframe[hgvs_pro_column])
        assert prefixes == self.pro_prefixes

    def test_splice_prefixes(self):
        prefixes = generate_variant_prefixes(self.dataframe[hgvs_splice_column])
        assert prefixes == self.splice_prefixes

    def test_unrecognized_column_prefixes(self):
        with self.assertRaises(ValueError):
            generate_variant_prefixes(self.dataframe["extra"])


class TestValidateVariantColumn(DfTestCase):
    def setUp(self):
        super().setUp()

    def test_invalid_column_type_index(self):
        with self.assertRaises(ValidationError):
            validate_variant_column(self.dataframe[required_score_column], True)

    def test_invalid_column_type(self):
        with self.assertRaises(ValidationError):
            validate_variant_column(self.dataframe[required_score_column], False)

    def test_null_values_type_index(self):
        self.dataframe[hgvs_nt_column].iloc[1] = pd.NA
        with self.assertRaises(ValidationError):
            validate_variant_column(self.dataframe.iloc[0, :], True)

    def test_null_values_type(self):
        self.dataframe[hgvs_nt_column].iloc[1] = pd.NA
        validate_variant_column(self.dataframe[hgvs_nt_column], False)

    def test_nonunique_values_index(self):
        self.dataframe["dup_col"] = ["p.Met1Leu", "p.Met1Leu"]
        with self.assertRaises(ValidationError):
            validate_variant_column(self.dataframe["dup_col"], True)

    def test_nonunique_values(self):
        self.dataframe["dup_col"] = ["p.Met1Leu", "p.Met1Leu"]
        validate_variant_column(self.dataframe["dup_col"], False)

    def test_variant_column_is_valid(self):
        validate_variant_column(self.dataframe[hgvs_nt_column], True)


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


# Spoof the target sequence type
class NucleotideSequenceTestCase:
    def __init__(self):
        self.sequence = "ATG"
        self.sequence_type = "dna"


class ProteinSequenceTestCase:
    def __init__(self):
        self.sequence = "MTG"
        self.sequence_type = "protein"


class TestValidateTransgenicColumn(DfTestCase):
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

        self.valid_hgvs_columns_nt_only = [
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column),
            pd.Series(["m.1A>G", "m.1A>T"], name=hgvs_nt_column),
            pd.Series(["c.1A>G", "c.1A>T"], name=hgvs_nt_column),
            pd.Series(["n.1A>G", "n.1A>T"], name=hgvs_nt_column),
        ]

        self.valid_hgvs_columns_multi_target = [
            pd.Series(["test_nt:g.1A>G", "test_nt:g.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:m.1A>G", "test_nt:m.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:c.1A>G", "test_nt:c.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:n.1A>G", "test_nt:n.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:c.1A>G", "test_pt:c.1A>T"], name=hgvs_splice_column),
            pd.Series(["test_pt:p.Met1Val", "test_pt:p.Met1Leu"], name=hgvs_pro_column),
            pd.Series(["test_nt:p.Met1Val", "test_pt:p.Met1Leu"], name=hgvs_pro_column),
            pd.Series(["test_nt:p.Met1Val", "test_nt:p.Met1Leu"], name=hgvs_pro_column),
        ]

        self.valid_hgvs_columns_nt_only_multi_target = [
            pd.Series(["test_nt:g.1A>G", "test_nt:g.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:m.1A>G", "test_nt:m.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:c.1A>G", "test_nt:c.1A>T"], name=hgvs_nt_column),
            pd.Series(["test_nt:n.1A>G", "test_nt:n.1A>T"], name=hgvs_nt_column),
        ]

        self.valid_hgvs_columns_invalid_names = [
            pd.Series(["g.1A>G", "g.1A>T"], name="invalid_column_name"),
            pd.Series(["p.Met1Val", "p.Met1Leu"], name="invalid_column_name"),
        ]

        self.valid_hgvs_columns_invalid_names_multi_target = [
            pd.Series(["test_nt:g.1A>G", "test_nt:g.1A>T"], name="invalid_column_name"),
            pd.Series(["test_pt:p.Met1Val", "test_pt:p.Met1Leu"], name="invalid_column_name"),
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

        self.valid_hgvs_columns_invalid_for_index_multi_target = [
            # missing data
            pd.Series(["test_nt:c.1A>G", None], name=hgvs_nt_column),
            pd.Series([None, "test_pt:p.Met1Val"], name=hgvs_pro_column),
            pd.Series([None, None], name=hgvs_nt_column),
            pd.Series([None, None], name=hgvs_pro_column),
            # duplicate rows
            pd.Series(["test_nt:c.1A>G", "test_nt:c.1A>G"], name=hgvs_nt_column),
            pd.Series(["test_nt:p.Met1Val", "test_nt:p.Met1Val"], name=hgvs_pro_column),
        ]

        self.invalid_hgvs_columns_by_name = [
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_splice_column),
            pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_pro_column),
            pd.Series(["c.1A>G", "c.1A>T"], name=hgvs_pro_column),
            pd.Series(["n.1A>G", "n.1A>T"], name=hgvs_pro_column),
            pd.Series(["p.Met1Val", "p.Met1Leu"], name=hgvs_nt_column),
        ]

        self.invalid_hgvs_columns_by_name_multi_target = [
            pd.Series(["test_nt:g.1A>G", "test_nt:g.1A>T"], name=hgvs_splice_column),
            pd.Series(["test_pt:g.1A>G", "test_pt:g.1A>T"], name=hgvs_pro_column),
            pd.Series(["test_nt:c.1A>G", "test_pt:c.1A>T"], name=hgvs_pro_column),
            pd.Series(["test_nt:n.1A>G", "test_nt:n.1A>T"], name=hgvs_pro_column),
            pd.Series(["test_nt:p.Met1Val", "test_nt:p.Met1Leu"], name=hgvs_nt_column),
            pd.Series(["test_nt:p.Met1Val", "test_pt:p.Met1Leu"], name=hgvs_nt_column),
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

        self.invalid_hgvs_columns_by_contents_multi_target = [
            pd.Series(["test_nt:r.1a>g", "test_nt:r.1a>u"], name=hgvs_splice_column),  # rna not allowed
            pd.Series(["test_nt:r.1a>g", "test_nt:r.1a>u"], name=hgvs_nt_column),  # rna not allowed
            pd.Series(["bad_label:r.1a>g", "test_nt:r.1a>u"], name=hgvs_nt_column),  # invalid label
            pd.Series(["test_nt:c.1A>G", "test_nt:c.5A>T"], name=hgvs_nt_column),  # out of bounds for target
            pd.Series(["test_nt:c.1A>G", "test_nt:_wt"], name=hgvs_nt_column),  # old special variant
            pd.Series(["test_pt:p.Met1Leu", "test_nt:_sy"], name=hgvs_pro_column),  # old special variant
            pd.Series(["test_nt:n.1A>G", "test_nt:c.1A>T"], name=hgvs_nt_column),  # mixed prefix
            pd.Series(["test_nt:c.1A>G", "test_pt:p.Met1Leu"], name=hgvs_pro_column),  # mixed types/prefix
            pd.Series(["test_pt:c.1A>G", "bad_label:p.Met1Leu"], name=hgvs_pro_column),  # invalid label
            pd.Series(["test_nt:c.1A>G", 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_splice_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_pro_column),  # contains numeric
        ]

        self.nt_sequence_test_case = NucleotideSequenceTestCase()
        self.pt_sequence_test_case = ProteinSequenceTestCase()

    def test_valid_columns_single_target(self):
        for column in self.valid_hgvs_columns:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=False,
                    targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                )
        for column in self.valid_hgvs_columns_invalid_for_index:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=False,
                    targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                )

    def test_valid_columns_multi_target(self):
        for column in self.valid_hgvs_columns_multi_target:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=False,
                    targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                )
        for column in self.valid_hgvs_columns_invalid_for_index_multi_target:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=False,
                    targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                )

    # Test when supplied targets do not contain a DNA sequence (only valid for hgvs_nt col)
    def test_valid_columns_invalid_supplied_targets(self):
        for column in self.valid_hgvs_columns_nt_only:
            with self.subTest(column=column):
                with self.assertRaises(ValueError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_pt": self.pt_sequence_test_case},  # type: ignore
                    )

    # Test when multiple supplied targets do not contain a DNA sequence (only valid for hgvs_nt col)
    def test_valid_columns_invalid_supplied_targets_multi_target(self):
        for column in self.valid_hgvs_columns_nt_only_multi_target:
            with self.subTest(column=column):
                with self.assertRaises(ValueError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_pt": self.pt_sequence_test_case, "test_pt_2": self.pt_sequence_test_case},  # type: ignore
                    )

    def test_valid_columns_invalid_column_name(self):
        for column in self.valid_hgvs_columns_invalid_names:
            with self.subTest(column=column):
                with self.assertRaises(ValueError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )

    def test_valid_columns_invalid_column_name_multi_target(self):
        for column in self.valid_hgvs_columns_invalid_names_multi_target:
            with self.subTest(column=column):
                with self.assertRaises(ValueError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )

    def test_index_columns(self):
        for column in self.valid_hgvs_columns:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=True,
                    targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                )
        for column in self.valid_hgvs_columns_invalid_for_index:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )

    def test_index_columns_multi_target(self):
        for column in self.valid_hgvs_columns_multi_target:
            with self.subTest(column=column):
                validate_hgvs_transgenic_column(
                    column,
                    is_index=True,
                    targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                )
        for column in self.valid_hgvs_columns_invalid_for_index_multi_target:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )

    def test_invalid_column_values(self):
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=False,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )

    def test_invalid_column_values_multi_target(self):
        for column in self.invalid_hgvs_columns_by_contents_multi_target:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=False,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )
        for column in self.invalid_hgvs_columns_by_contents_multi_target:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )

    def test_valid_column_values_wrong_column_name(self):
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=False,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case},  # type: ignore
                    )

    def test_valid_column_values_wrong_column_name_multi_target(self):
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=False,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_hgvs_transgenic_column(
                        column,
                        is_index=True,
                        targets={"test_nt": self.nt_sequence_test_case, "test_pt": self.pt_sequence_test_case},  # type: ignore
                    )


# Spoof the accession type
class AccessionTestCase:
    def __init__(self):
        self.accession = VALID_ACCESSION


class TestValidateHgvsGenomicColumn(DfTestCase):
    def setUp(self):
        super().setUp()

        self.accession_test_case = AccessionTestCase()

        self.valid_hgvs_column = pd.Series(
            [f"{VALID_ACCESSION}:c.1G>A", f"{VALID_ACCESSION}:c.2A>T"], name=hgvs_nt_column
        )
        self.missing_data = pd.Series([f"{VALID_ACCESSION}:c.3T>G", None], name=hgvs_nt_column)
        self.duplicate_data = pd.Series([f"{VALID_ACCESSION}:c.4A>G", f"{VALID_ACCESSION}:c.4A>G"], name=hgvs_nt_column)

        self.invalid_hgvs_columns_by_name = [
            pd.Series([f"{VALID_ACCESSION}:g.1A>G", f"{VALID_ACCESSION}:g.1A>T"], name=hgvs_splice_column),
            pd.Series([f"{VALID_ACCESSION}:g.1A>G", f"{VALID_ACCESSION}:g.1A>T"], name=hgvs_pro_column),
            pd.Series([f"{VALID_ACCESSION}:c.1A>G", f"{VALID_ACCESSION}:c.1A>T"], name=hgvs_pro_column),
            pd.Series([f"{VALID_ACCESSION}:n.1A>G", f"{VALID_ACCESSION}:n.1A>T"], name=hgvs_pro_column),
            pd.Series([f"{VALID_ACCESSION}:p.Met1Val", f"{VALID_ACCESSION}:p.Met1Leu"], name=hgvs_nt_column),
        ]

        self.invalid_hgvs_columns_by_contents = [
            pd.Series(
                [f"{VALID_ACCESSION}:r.1a>g", f"{VALID_ACCESSION}:r.1a>u"], name=hgvs_splice_column
            ),  # rna not allowed
            pd.Series(
                [f"{VALID_ACCESSION}:r.1a>g", f"{VALID_ACCESSION}:r.1a>u"], name=hgvs_nt_column
            ),  # rna not allowed
            pd.Series(
                [f"{VALID_ACCESSION}:c.1A>G", f"{VALID_ACCESSION}:c.5A>T"], name=hgvs_nt_column
            ),  # out of bounds for target
            pd.Series([f"{VALID_ACCESSION}:c.1A>G", "_wt"], name=hgvs_nt_column),  # old special variant
            pd.Series([f"{VALID_ACCESSION}:p.Met1Leu", "_sy"], name=hgvs_pro_column),  # old special variant
            pd.Series([f"{VALID_ACCESSION}:n.1A>G", f"{VALID_ACCESSION}:c.1A>T"], name=hgvs_nt_column),  # mixed prefix
            pd.Series(
                [f"{VALID_ACCESSION}:c.1A>G", f"{VALID_ACCESSION}:p.Met1Leu"], name=hgvs_pro_column
            ),  # mixed types/prefix
            pd.Series(["c.1A>G", "p.Met1Leu"], name=hgvs_pro_column),  # variants should be fully qualified
            pd.Series([f"{VALID_ACCESSION}:c.1A>G", 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_nt_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_splice_column),  # contains numeric
            pd.Series([1.0, 2.5], name=hgvs_pro_column),  # contains numeric
        ]

    def test_valid_variant(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column, is_index=False, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_valid_variant_valid_missing(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ):
            validate_hgvs_genomic_column(
                self.missing_data, is_index=False, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_valid_variant_valid_duplicate(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ):
            validate_hgvs_genomic_column(
                self.missing_data, is_index=False, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_valid_variant_index(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider,
            "_get_transcript",
            return_value=TEST_CDOT_TRANSCRIPT,
        ):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column, is_index=True, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_valid_variant_invalid_missing_index(self):
        with (
            self.assertRaises(ValidationError),
            patch.object(
                cdot.hgvs.dataproviders.RESTDataProvider,
                "_get_transcript",
                return_value=TEST_CDOT_TRANSCRIPT,
            ),
        ):
            validate_hgvs_genomic_column(
                self.missing_data, is_index=True, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_valid_variant_invalid_duplicate_index(self):
        with (
            self.assertRaises(ValidationError),
            patch.object(
                cdot.hgvs.dataproviders.RESTDataProvider,
                "_get_transcript",
                return_value=TEST_CDOT_TRANSCRIPT,
            ),
        ):
            validate_hgvs_genomic_column(
                self.duplicate_data, is_index=True, targets=[self.accession_test_case], hdp=self.human_data_provider
            )  # type: ignore

    def test_invalid_column_values(self):
        for column in self.invalid_hgvs_columns_by_contents:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider,
                    "_get_transcript",
                    return_value=TEST_CDOT_TRANSCRIPT,
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.human_data_provider,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_contents:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider,
                    "_get_transcript",
                    return_value=TEST_CDOT_TRANSCRIPT,
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.human_data_provider,  # type: ignore
                )

    def test_valid_column_values_wrong_column_name(self):
        for column in self.invalid_hgvs_columns_by_name:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider,
                    "_get_transcript",
                    return_value=TEST_CDOT_TRANSCRIPT,
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.human_data_provider,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_name:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider,
                    "_get_transcript",
                    return_value=TEST_CDOT_TRANSCRIPT,
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.human_data_provider,  # type: ignore
                )

    # TODO: Test multiple targets
