from unittest import TestCase
from unittest.mock import Mock
import pandas as pd

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    required_score_column,
)
from mavedb.lib.validation.dataframe.column import (
    construct_target_sequence_mappings,
    infer_column_type,
    generate_variant_prefixes,
    validate_data_column,
    validate_hgvs_column_properties,
    validate_variant_formatting,
    validate_variant_column,
)

from tests.validation.dataframe.conftest import DfTestCase


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
        self.dataframe.iloc[1, self.dataframe.columns.get_loc(hgvs_nt_column)] = pd.NA
        with self.assertRaises(ValidationError):
            validate_variant_column(self.dataframe.iloc[0, :], True)

    def test_null_values_type(self):
        self.dataframe.iloc[1, self.dataframe.columns.get_loc(hgvs_nt_column)] = pd.NA
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


class TestValidateHgvsColumnProperties(TestCase):
    def setUp(self):
        self.dna_observed = ["dna"]
        self.protein_observed = ["protein"]
        self.mixed_observed = ["dna", "protein"]

    def test_valid_dna_column(self):
        column = pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column)
        validate_hgvs_column_properties(column, self.dna_observed)

    def test_invalid_dna_column(self):
        column = pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column)
        with self.assertRaises(ValueError):
            validate_hgvs_column_properties(column, self.protein_observed)

    def test_valid_splice_column(self):
        column = pd.Series(["c.1-2A>G", "c.1-2A>T"], name=hgvs_splice_column)
        validate_hgvs_column_properties(column, self.mixed_observed)

    def test_valid_protein_column(self):
        column = pd.Series(["p.Met1Leu", "p.Met1Val"], name=hgvs_pro_column)
        validate_hgvs_column_properties(column, self.mixed_observed)

    def test_invalid_column_name(self):
        column = pd.Series(["x.1A>G", "x.1A>T"], name="invalid_column")
        with self.assertRaises(ValueError):
            validate_hgvs_column_properties(column, self.mixed_observed)


class TestConstructTargetSequenceMappings(TestCase):
    def setUp(self):
        mock_seq1, mock_seq2, mock_seq3 = Mock(), Mock(), Mock()
        mock_seq1.sequence = "ATGCGT"
        mock_seq1.sequence_type = "dna"
        mock_seq2.sequence = "MR"
        mock_seq2.sequence_type = "protein"
        mock_seq3.sequence = None
        mock_seq3.sequence_type = "dna"

        self.targets = {
            "target1": mock_seq1,
            "target2": mock_seq2,
            "target3": mock_seq3,
        }

    def test_nt_column(self):
        column = pd.Series(["g.1A>G", "g.1A>T"], name=hgvs_nt_column)
        expected = {
            "target1": "ATGCGT",
            "target2": "MR",
            "target3": None,
        }
        result = construct_target_sequence_mappings(column, self.targets)
        self.assertEqual(result, expected)

    def test_splice_column(self):
        column = pd.Series(["c.1-2A>G", "c.1-2A>T"], name=hgvs_splice_column)
        expected = {
            "target1": None,
            "target2": None,
            "target3": None,
        }
        result = construct_target_sequence_mappings(column, self.targets)
        self.assertEqual(result, expected)

    def test_pro_column(self):
        column = pd.Series(["p.Met1Leu", "p.Met1Val"], name=hgvs_pro_column)
        expected = {
            "target1": "MR",
            "target2": "MR",
            "target3": None,
        }
        result = construct_target_sequence_mappings(column, self.targets)
        self.assertEqual(result, expected)

    def test_invalid_column_name(self):
        column = pd.Series(["x.1A>G", "x.1A>T"], name="invalid_column")
        with self.assertRaises(ValueError):
            construct_target_sequence_mappings(column, self.targets)
