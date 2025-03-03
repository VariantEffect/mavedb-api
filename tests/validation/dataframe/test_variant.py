import pytest
import pandas as pd
import unittest
from unittest.mock import Mock, patch

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
)
from mavedb.lib.validation.dataframe.variant import (
    validate_guide_sequence_column,
    validate_hgvs_transgenic_column,
    validate_hgvs_genomic_column,
    parse_genomic_variant,
    parse_transgenic_variant,
    validate_observed_sequence_types,
    validate_hgvs_prefix_combinations,
)
from mavedb.lib.validation.exceptions import ValidationError

from tests.helpers.constants import VALID_ACCESSION, TEST_CDOT_TRANSCRIPT
from tests.validation.dataframe.conftest import DfTestCase


try:
    import hgvs  # noqa: F401
    import cdot.hgvs.dataproviders  # noqa: F401

    HGVS_INSTALLED = True
except ModuleNotFoundError:
    HGVS_INSTALLED = False


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


class GenomicColumnValidationTestCase(DfTestCase):
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

        self.invalid_hgvs_columns_by_contents_under_strict_validation = [
            pd.Series(
                [f"{VALID_ACCESSION}:c.1A>G", f"{VALID_ACCESSION}:c.5A>T"], name=hgvs_nt_column
            ),  # out of bounds for target
        ]


class TestValidateHgvsGenomicColumn(GenomicColumnValidationTestCase):
    # Identical behavior for installed/uninstalled HGVS
    def test_valid_variant_invalid_missing_index(self):
        with (
            self.assertRaises(ValidationError),
        ):
            validate_hgvs_genomic_column(
                self.missing_data,
                is_index=True,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore

    # Identical behavior for installed/uninstalled HGVS
    def test_valid_variant_invalid_duplicate_index(self):
        with (
            self.assertRaises(ValidationError),
        ):
            validate_hgvs_genomic_column(
                self.duplicate_data,
                is_index=True,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore


@unittest.skipUnless(HGVS_INSTALLED, "HGVS module not installed")
@pytest.fixture
def patched_data_provider_class_attr(request, data_provider):
    """
    Sets the `human_data_provider` attribute on the class from the requesting
    test context to the `data_provider` fixture. This allows fixture use across
    the `unittest.TestCase` class.
    """
    request.cls.patched_human_data_provider = data_provider


@unittest.skipUnless(HGVS_INSTALLED, "HGVS module not installed")
@pytest.mark.usefixtures("patched_data_provider_class_attr")
class TestValidateHgvsGenomicColumnHgvsInstalled(GenomicColumnValidationTestCase):
    def test_valid_variant(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
        ):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.patched_human_data_provider,
            )  # type: ignore

    def test_valid_variant_valid_missing(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
        ):
            validate_hgvs_genomic_column(
                self.missing_data,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.patched_human_data_provider,
            )  # type: ignore

    def test_valid_variant_valid_duplicate(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
        ):
            validate_hgvs_genomic_column(
                self.missing_data,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.patched_human_data_provider,
            )  # type: ignore

    def test_valid_variant_index(self):
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
        ):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column,
                is_index=True,
                targets=[self.accession_test_case],
                hdp=self.patched_human_data_provider,
            )  # type: ignore

    def test_invalid_column_values(self):
        for column in (
            self.invalid_hgvs_columns_by_contents + self.invalid_hgvs_columns_by_contents_under_strict_validation
        ):
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.patched_human_data_provider,  # type: ignore
                )
        for column in (
            self.invalid_hgvs_columns_by_contents + self.invalid_hgvs_columns_by_contents_under_strict_validation
        ):
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.patched_human_data_provider,  # type: ignore
                )

    def test_valid_column_values_wrong_column_name(self):
        for column in self.invalid_hgvs_columns_by_name:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.patched_human_data_provider,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_name:
            with (
                self.subTest(column=column),
                self.assertRaises(ValidationError),
                patch.object(
                    cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT
                ),
            ):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.patched_human_data_provider,  # type: ignore
                )

    # TODO: Test multiple targets


@unittest.skipIf(HGVS_INSTALLED, "HGVS module installed")
class TestValidateHgvsGenomicColumnHgvsNotInstalled(GenomicColumnValidationTestCase):
    def test_valid_variant_strict_validation(self):
        with self.assertRaises(ModuleNotFoundError):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore

    def test_valid_variant_limited_validation(self):
        validate_hgvs_genomic_column(
            self.valid_hgvs_column, is_index=False, targets=[self.accession_test_case], hdp=None
        )  # type: ignore

    def test_valid_variant_valid_missing_strict_validation(self):
        with self.assertRaises(ModuleNotFoundError):
            validate_hgvs_genomic_column(
                self.missing_data,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore

    def test_valid_variant_valid_missing_limited_validation(self):
        validate_hgvs_genomic_column(self.missing_data, is_index=False, targets=[self.accession_test_case], hdp=None)  # type: ignore

    def test_valid_variant_valid_duplicate_strict_validation(self):
        with self.assertRaises(ModuleNotFoundError):
            validate_hgvs_genomic_column(
                self.missing_data,
                is_index=False,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore

    def test_valid_variant_valid_duplicate_limited_validation(self):
        validate_hgvs_genomic_column(self.missing_data, is_index=False, targets=[self.accession_test_case], hdp=None)  # type: ignore

    def test_valid_variant_index_strict_validation(self):
        with self.assertRaises(ModuleNotFoundError):
            validate_hgvs_genomic_column(
                self.valid_hgvs_column,
                is_index=True,
                targets=[self.accession_test_case],
                hdp=self.mocked_human_data_provider,
            )  # type: ignore

    def test_valid_variant_index_limited_validation(self):
        validate_hgvs_genomic_column(
            self.valid_hgvs_column, is_index=True, targets=[self.accession_test_case], hdp=None
        )  # type: ignore

    def test_invalid_column_values_strict_validation(self):
        for column in (
            self.invalid_hgvs_columns_by_contents + self.invalid_hgvs_columns_by_contents_under_strict_validation
        ):
            with self.subTest(column=column), self.assertRaises((ValidationError, ModuleNotFoundError)):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.mocked_human_data_provider,  # type: ignore
                )
        for column in (
            self.invalid_hgvs_columns_by_contents + self.invalid_hgvs_columns_by_contents_under_strict_validation
        ):
            with self.subTest(column=column), self.assertRaises((ValidationError, ModuleNotFoundError)):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.mocked_human_data_provider,  # type: ignore
                )

    def test_invalid_column_values_limited_validation(self):
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=None,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_contents:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=None,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_contents_under_strict_validation:
            with self.subTest(column=column):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=None,  # type: ignore
                )

    def test_valid_column_values_wrong_column_name_strict_validation(self):
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=self.mocked_human_data_provider,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=self.mocked_human_data_provider,  # type: ignore
                )

    def test_valid_column_values_wrong_column_name_limited_validation(self):
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=False,
                    targets=[self.accession_test_case],
                    hdp=None,  # type: ignore
                )
        for column in self.invalid_hgvs_columns_by_name:
            with self.subTest(column=column), self.assertRaises(ValidationError):
                validate_hgvs_genomic_column(
                    column,
                    is_index=True,
                    targets=[self.accession_test_case],
                    hdp=None,  # type: ignore
                )


class TestParseGenomicVariant(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.parser = Mock()
        self.validator = Mock()
        self.parser.parse.return_value = "irrelevant"
        self.validator.validate.return_value = True

        self.falsy_variant_strings = [None, ""]
        self.valid_hgvs_column = pd.Series(
            [f"{VALID_ACCESSION}:c.1G>A", f"{VALID_ACCESSION}:c.2A>T"], name=hgvs_nt_column
        )
        self.invalid_hgvs_column = pd.Series(
            [f"{VALID_ACCESSION}:c.1laksdfG>A", f"{VALID_ACCESSION}:c.2kadlfjA>T"], name=hgvs_nt_column
        )


@unittest.skipUnless(HGVS_INSTALLED, "HGVS module not installed")
class TestParseGenomicVariantHgvsInstalled(TestParseGenomicVariant):
    def test_parse_genomic_variant_nonetype_variant_string(self):
        for variant_string in self.falsy_variant_strings:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_genomic_variant(0, None, self.parser, self.validator)
                assert valid
                assert error is None

    def test_parse_valid_hgvs_variant(self):
        for variant_string in self.valid_hgvs_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_genomic_variant(0, self.valid_hgvs_column[0], self.parser, self.validator)
                assert valid
                assert error is None

    def test_parse_invalid_hgvs_variant(self):
        from hgvs.exceptions import HGVSError

        self.validator.validate.side_effect = HGVSError("Invalid variant")

        for variant_string in self.invalid_hgvs_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_genomic_variant(0, self.valid_hgvs_column[0], self.parser, self.validator)
                assert not valid
                assert "Failed to parse row 0 with HGVS exception:" in error


@unittest.skipIf(HGVS_INSTALLED, "HGVS module installed")
class TestParseGenomicVariantHgvsNotInstalled(TestParseGenomicVariant):
    def test_parse_genomic_variant_nonetype_variant_string(self):
        for variant_string in self.falsy_variant_strings:
            with self.subTest(variant_string=variant_string), self.assertRaises(ModuleNotFoundError):
                parse_genomic_variant(0, None, self.parser, self.validator)

    def test_parse_valid_hgvs_variant(self):
        for variant_string in self.valid_hgvs_column:
            with self.subTest(variant_string=variant_string), self.assertRaises(ModuleNotFoundError):
                parse_genomic_variant(0, self.valid_hgvs_column[0], self.parser, self.validator)

    def test_parse_invalid_hgvs_variant(self):
        for variant_string in self.invalid_hgvs_column:
            with self.subTest(variant_string=variant_string), self.assertRaises(ModuleNotFoundError):
                parse_genomic_variant(0, self.valid_hgvs_column[0], self.parser, self.validator)


class TestParseTransgenicVariant(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.target_sequences = {f"{VALID_ACCESSION}": "ATGC"}

        self.falsy_variant_strings = [None, ""]
        self.valid_fully_qualified_transgenic_column = pd.Series(
            [f"{VALID_ACCESSION}:c.1A>G", f"{VALID_ACCESSION}:c.2T>G {VALID_ACCESSION}:c.2T>G"], name=hgvs_nt_column
        )
        self.valid_basic_transgenic_column = pd.Series(["c.1A>G", "c.2T>G c.2T>G"], name=hgvs_nt_column)
        self.invalid_transgenic_column = pd.Series(["123A>X", "NM_001:123A>Y"], name=hgvs_nt_column)
        self.mismatched_transgenic_column = pd.Series(["c.1T>G", "c.2A>G"], name=hgvs_nt_column)

    def test_parse_transgenic_variant_nonetype_variant_string(self):
        for variant_string in self.falsy_variant_strings:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_transgenic_variant(0, None, self.target_sequences, is_fully_qualified=False)
                assert valid
                assert error is None

    def test_parse_valid_fully_qualified_transgenic_variant(self):
        for variant_string in self.valid_fully_qualified_transgenic_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_transgenic_variant(
                    0, variant_string, self.target_sequences, is_fully_qualified=True
                )
                assert valid
                assert error is None

    def test_parse_valid_basic_transgenic_variant(self):
        for variant_string in self.valid_basic_transgenic_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_transgenic_variant(
                    0, variant_string, self.target_sequences, is_fully_qualified=False
                )
                assert valid
                assert error is None

    def test_parse_invalid_transgenic_variant(self):
        for variant_string in self.invalid_transgenic_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_transgenic_variant(
                    0, variant_string, self.target_sequences, is_fully_qualified=False
                )
                assert not valid
                assert "invalid variant string" in error

    def test_parse_mismatched_transgenic_variant(self):
        for variant_string in self.mismatched_transgenic_column:
            with self.subTest(variant_string=variant_string):
                valid, error = parse_transgenic_variant(
                    0, variant_string, self.target_sequences, is_fully_qualified=False
                )
                assert not valid
                assert "target sequence mismatch" in error


class TestValidateGuideSequenceColumn(DfTestCase):
    def setUp(self):
        super().setUp()

        self.valid_guide_sequences = [
            pd.Series(["ATG", "TGA"], name="guide_sequence"),
            pd.Series(["ATGC", "TGAC"], name="guide_sequence"),
            pd.Series(["ATGCG", "TGACG"], name="guide_sequence"),
        ]

        self.invalid_guide_sequences = [
            pd.Series(["ATG", "XYZ"], name="guide_sequence"),  # invalid DNA sequence
            pd.Series(["123", "123"], name="guide_sequence"),  # contains numeric
        ]

        self.invalid_index_guide_sequences = [
            pd.Series(["ATG", None], name="guide_sequence"),  # contains None value
            pd.Series(["ATG", "ATG"], name="guide_sequence"),  # identical sequences
        ]

        self.accession_test_case = AccessionTestCase()

    def test_valid_guide_sequences(self):
        for column in self.valid_guide_sequences + self.invalid_index_guide_sequences:
            with self.subTest(column=column):
                validate_guide_sequence_column(
                    column,
                    is_index=False,
                )

    def test_invalid_guide_sequences(self):
        for column in self.invalid_guide_sequences:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_guide_sequence_column(
                        column,
                        is_index=False,
                    )

    def test_valid_guide_sequences_index(self):
        for column in self.valid_guide_sequences:
            with self.subTest(column=column):
                validate_guide_sequence_column(
                    column,
                    is_index=True,
                )

    def test_invalid_guide_sequences_index(self):
        for column in self.invalid_guide_sequences + self.invalid_index_guide_sequences:
            with self.subTest(column=column):
                with self.assertRaises(ValidationError):
                    validate_guide_sequence_column(
                        column,
                        is_index=True,
                    )


class TestValidateObservedSequenceTypes(unittest.TestCase):
    def setUp(self):
        super().setUp()

        mock_valid_target1 = Mock()
        mock_valid_target2 = Mock()
        mock_valid_target1.sequence_type = "dna"
        mock_valid_target1.sequence = "ATGC"
        mock_valid_target2.sequence_type = "protein"
        mock_valid_target2.sequence = "NM"
        self.valid_targets = {
            "NM_001": mock_valid_target1,
            "NM_002": mock_valid_target2,
        }

        mock_invalid_target1 = Mock()
        mock_invalid_target2 = Mock()
        mock_invalid_target1.sequence_type = "dna"
        mock_invalid_target1.sequence = "ATGC"
        mock_invalid_target2.sequence_type = "invalid"
        mock_invalid_target2.sequence = "ABCD"
        self.invalid_targets = {
            "NM_001": mock_invalid_target1,
            "NM_002": mock_invalid_target2,
        }

    def test_validate_observed_sequence_types(self):
        observed_sequence_types = validate_observed_sequence_types(self.valid_targets)
        assert observed_sequence_types == ["dna", "protein"]

    def test_validate_invalid_observed_sequence_types(self):
        with self.assertRaises(ValueError):
            validate_observed_sequence_types(self.invalid_targets)

    def test_validate_observed_sequence_types_no_targets(self):
        with self.assertRaises(ValueError):
            validate_observed_sequence_types({})


class TestValidateHgvsPrefixCombinations(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.valid_combinations = [
            ("c", None, None, False),
            ("g", "n", None, False),
            ("g", "c", "p", False),
            ("n", None, None, True),
        ]

        self.invalid_combinations = [
            ("n", "n", None, False),
            ("c", "n", None, False),
            ("g", "n", "p", False),
            ("g", "c", None, False),
            ("n", None, "p", False),
            ("g", None, None, True),  # invalid nucleotide prefix when transgenic
        ]

        self.invalid_prefix_values = [
            ("x", None, None, False),  # invalid nucleotide prefix
            ("c", "x", None, False),  # invalid splice prefix
            ("c", None, "x", False),  # invalid protein prefix
        ]

    def test_valid_combinations(self):
        for hgvs_nt, hgvs_splice, hgvs_pro, transgenic in self.valid_combinations:
            with self.subTest(hgvs_nt=hgvs_nt, hgvs_splice=hgvs_splice, hgvs_pro=hgvs_pro, transgenic=transgenic):
                validate_hgvs_prefix_combinations(hgvs_nt, hgvs_splice, hgvs_pro, transgenic)

    def test_invalid_combinations(self):
        for hgvs_nt, hgvs_splice, hgvs_pro, transgenic in self.invalid_combinations:
            with self.subTest(hgvs_nt=hgvs_nt, hgvs_splice=hgvs_splice, hgvs_pro=hgvs_pro, transgenic=transgenic):
                with self.assertRaises(ValidationError):
                    validate_hgvs_prefix_combinations(hgvs_nt, hgvs_splice, hgvs_pro, transgenic)

    def test_invalid_prefix_values(self):
        for hgvs_nt, hgvs_splice, hgvs_pro, transgenic in self.invalid_prefix_values:
            with self.subTest(hgvs_nt=hgvs_nt, hgvs_splice=hgvs_splice, hgvs_pro=hgvs_pro, transgenic=transgenic):
                with self.assertRaises(ValueError):
                    validate_hgvs_prefix_combinations(hgvs_nt, hgvs_splice, hgvs_pro, transgenic)
