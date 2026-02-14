# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.util module.

This module tests utility functions used by annotation workflows, including
variation extraction, annotation eligibility checks, and sequence feature
resolution.
"""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.util import (
    _can_annotate_variant_base_assumptions,
    _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
    score_calibration_may_be_used_for_annotation,
    select_strongest_functional_calibration,
    select_strongest_pathogenicity_calibration,
    sequence_feature_for_mapped_variant,
    serialize_evidence_items,
    variation_from_mapped_variant,
    vrs_object_from_mapped_variant,
)
from tests.helpers.constants import TEST_SEQUENCE_LOCATION_ACCESSION, TEST_VALID_POST_MAPPED_VRS_ALLELE


@pytest.mark.unit
class TestVariationExtractionUnit:
    @pytest.mark.parametrize(
        "variation_version",
        [{"variation": TEST_VALID_POST_MAPPED_VRS_ALLELE}, TEST_VALID_POST_MAPPED_VRS_ALLELE],
        ids=["vrs13_wrapped_variation", "vrs2_direct_allele"],
    )
    def test_variation_from_mapped_variant_post_mapped_variation(self, mock_mapped_variant, variation_version):
        mock_mapped_variant.post_mapped = variation_version

        result = variation_from_mapped_variant(mock_mapped_variant).model_dump()

        assert result["location"]["id"] == TEST_SEQUENCE_LOCATION_ACCESSION
        assert result["location"]["start"] == 5
        assert result["location"]["end"] == 6

    def test_variation_from_mapped_variant_no_post_mapped(self, mock_mapped_variant):
        mock_mapped_variant.post_mapped = None

        with pytest.raises(MappingDataDoesntExistException):
            variation_from_mapped_variant(mock_mapped_variant)

    def test_vrs_object_from_mapped_variant_handles_haplotype_member_list(self):
        mapping_results = {
            "type": "Haplotype",
            "members": [TEST_VALID_POST_MAPPED_VRS_ALLELE, TEST_VALID_POST_MAPPED_VRS_ALLELE],
        }

        result = vrs_object_from_mapped_variant(mapping_results).model_dump()

        assert result["type"] == "CisPhasedBlock"
        assert len(result["members"]) == 2


@pytest.mark.unit
class TestBaseAnnotationAssumptionsUnit:
    def test_base_assumption_check_returns_false_when_score_is_none(self, mock_mapped_variant):
        mock_mapped_variant.variant.data = {"score_data": {"score": None}}

        assert _can_annotate_variant_base_assumptions(mock_mapped_variant) is False

    def test_base_assumption_check_returns_true_when_all_conditions_met(self, mock_mapped_variant):
        assert _can_annotate_variant_base_assumptions(mock_mapped_variant) is True


@pytest.mark.unit
class TestScoreCalibrationMayBeUsedForAnnotation:
    def test_returns_false_for_research_use_only_when_not_allowed(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        calibration = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations[0]
        calibration.research_use_only = True

        assert (
            score_calibration_may_be_used_for_annotation(
                calibration,
                annotation_type="functional",
                allow_research_use_only_calibrations=False,
            )
            is False
        )

    def test_returns_true_for_research_use_only_when_allowed(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        calibration = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations[0]
        calibration.research_use_only = True

        assert (
            score_calibration_may_be_used_for_annotation(
                calibration,
                annotation_type="functional",
                allow_research_use_only_calibrations=True,
            )
            is True
        )

    def test_returns_false_when_functional_classifications_missing(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        calibration = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations[0]
        calibration.functional_classifications = []

        assert score_calibration_may_be_used_for_annotation(calibration, annotation_type="functional") is False

    def test_returns_false_for_pathogenicity_without_acmg_classifications(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        calibration = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations[
            0
        ]
        acmg_removed = [deepcopy(fc) for fc in calibration.functional_classifications]
        for functional_classification in acmg_removed:
            functional_classification["acmgClassification"] = None
        calibration.functional_classifications = acmg_removed

        assert score_calibration_may_be_used_for_annotation(calibration, annotation_type="pathogenicity") is False

    def test_returns_true_for_pathogenicity_with_any_acmg_classification(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        calibration = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations[
            0
        ]

        assert score_calibration_may_be_used_for_annotation(calibration, annotation_type="pathogenicity") is True


@pytest.mark.unit
class TestVariantScoreCalibrationsHaveRequiredCalibrationsAndRangesForAnnotation:
    """
    Unit tests for the _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation function.
    This function is used by both functional and pathogenicity annotation checks, so we test it separately here to avoid duplication in the tests for those checks.
    """

    @pytest.mark.parametrize("kind", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_calibrations_are_none(self, mock_mapped_variant, kind):
        mock_mapped_variant.variant.score_set.score_calibrations = None
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
            is False
        )

    @pytest.mark.parametrize("kind", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_no_calibrations_present(self, mock_mapped_variant, kind):
        mock_mapped_variant.variant.score_set.score_calibrations = []
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
            is False
        )

    @pytest.mark.parametrize("annotation_type", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_all_calibrations_are_research_use_only_and_not_allowed(
        self, mock_mapped_variant_with_functional_calibration_score_set, annotation_type
    ):
        """Test that research use only calibrations are excluded by default."""
        # Make all calibrations research use only
        for (
            calibration
        ) in mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_functional_calibration_score_set, annotation_type
            )
            is False
        )

    @pytest.mark.parametrize(
        "kind,variant_fixture",
        [
            ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_true_when_research_use_only_calibrations_are_allowed(
        self, kind, variant_fixture, request
    ):
        """Test that research use only calibrations are included when explicitly allowed."""
        mock_mapped_variant = request.getfixturevalue(variant_fixture)
        # Make all calibrations research use only
        for calibration in mock_mapped_variant.variant.score_set.score_calibrations:
            calibration.primary = False
            calibration.research_use_only = True

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant, kind, allow_research_use_only_calibrations=True
            )
            is True
        )

    @pytest.mark.parametrize(
        "kind,variant_fixture",
        [
            ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_false_when_calibrations_present_with_empty_ranges(
        self, kind, variant_fixture, request
    ):
        mock_mapped_variant = request.getfixturevalue(variant_fixture)

        for calibration in mock_mapped_variant.variant.score_set.score_calibrations:
            calibration.functional_classifications = None

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
            is False
        )

    def test_pathogenicity_range_check_returns_false_when_no_acmg_calibration(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ):
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
            for fr in acmg_classification_removed:
                fr["acmgClassification"] = None

            calibration.functional_classifications = acmg_classification_removed

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is False
        )

    def test_pathogenicity_range_check_returns_true_when_some_acmg_calibration(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ):
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
            acmg_classification_removed[0]["acmgClassification"] = None

            calibration.functional_classifications = acmg_classification_removed

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is True
        )

    @pytest.mark.parametrize(
        "kind,variant_fixture",
        [
            ("functional", "mock_mapped_variant_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_mapped_variant_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_true_when_calibration_kind_exists_with_ranges(
        self, kind, variant_fixture, request
    ):
        mock_mapped_variant = request.getfixturevalue(variant_fixture)

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(mock_mapped_variant, kind)
            is True
        )

    def test_score_range_check_returns_true_when_mixed_research_use_calibrations_exist_functional(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test behavior with mixed research use only and regular calibrations for functional annotation."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations

        # If there's only one calibration, add another for testing
        if len(calibrations) == 1:
            # Create a copy of the existing calibration
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        # Make the first one research use only, leave the second as regular
        calibrations[0].research_use_only = True
        calibrations[1].research_use_only = False

        # Should return True because at least one non-research-only calibration has valid classifications
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_functional_calibration_score_set, "functional"
            )
            is True
        )

    def test_score_range_check_returns_true_when_mixed_research_use_calibrations_exist_pathogenicity(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test behavior with mixed research use only and regular calibrations for pathogenicity annotation."""
        calibrations = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations

        # If there's only one calibration, add another for testing
        if len(calibrations) == 1:
            # Create a copy of the existing calibration
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        # Make the first one research use only, leave the second as regular
        calibrations[0].research_use_only = True
        calibrations[1].research_use_only = False

        # Should return True because at least one non-research-only calibration has valid classifications
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is True
        )

    def test_score_range_check_handles_mixed_functional_classifications(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        """Test behavior when some calibrations have functional classifications and some don't."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations

        # If there's only one calibration, add another for testing
        if len(calibrations) == 1:
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        # First calibration has functional classifications (should already exist)
        # Second calibration has no functional classifications
        calibrations[1].functional_classifications = None

        # Should return True because at least one calibration has valid functional classifications
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_functional_calibration_score_set, "functional"
            )
            is True
        )

    def test_pathogenicity_annotation_with_functional_classifications_but_no_acmg(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        """Test that pathogenicity annotation fails when functional classifications exist but have no ACMG classifications."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations

        # Remove ACMG classifications from all functional classifications
        for calibration in calibrations:
            if hasattr(calibration, "functional_classifications") and calibration.functional_classifications:
                acmg_classification_removed = [deepcopy(fc) for fc in calibration.functional_classifications]
                for fc in acmg_classification_removed:
                    if "acmgClassification" in fc:
                        fc["acmgClassification"] = None
                calibration.functional_classifications = acmg_classification_removed

        # Should return False because no ACMG classifications exist
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_functional_calibration_score_set, "pathogenicity"
            )
            is False
        )

    def test_functional_annotation_with_empty_functional_classifications_list(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        """Test that functional annotation fails when functional classifications list is empty."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations

        # Set functional classifications to empty list
        for calibration in calibrations:
            calibration.functional_classifications = []

        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant_with_functional_calibration_score_set, "functional"
            )
            is False
        )


@pytest.mark.unit
class TestPathogenicityAnnotationEligibilityUnit:
    def test_pathogenicity_range_check_returns_false_when_base_assumptions_fail(self, mock_mapped_variant):
        with patch("mavedb.lib.annotation.util._can_annotate_variant_base_assumptions", return_value=False):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

        assert result is False

    def test_pathogenicity_range_check_returns_false_when_pathogenicity_ranges_check_fails(self, mock_mapped_variant):
        with patch(
            "mavedb.lib.annotation.util._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
            return_value=False,
        ):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant)

        assert result is False

    def test_pathogenicity_range_check_returns_true_when_all_conditions_met(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ):
        assert (
            can_annotate_variant_for_pathogenicity_evidence(
                mock_mapped_variant_with_pathogenicity_calibration_score_set
            )
            is True
        )


@pytest.mark.unit
class TestFunctionalAnnotationEligibilityUnit:
    def test_functional_range_check_returns_false_when_base_assumptions_fail(self, mock_mapped_variant):
        with patch(
            "mavedb.lib.annotation.util._can_annotate_variant_base_assumptions",
            return_value=False,
        ):
            result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

        assert result is False

    def test_functional_range_check_returns_false_when_functional_classifications_check_fails(
        self, mock_mapped_variant
    ):
        with patch(
            "mavedb.lib.annotation.util._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
            return_value=False,
        ):
            result = can_annotate_variant_for_functional_statement(mock_mapped_variant)

        assert result is False

    def test_functional_range_check_returns_true_when_all_conditions_met(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        assert (
            can_annotate_variant_for_functional_statement(mock_mapped_variant_with_functional_calibration_score_set)
            is True
        )


@pytest.mark.unit
class TestSequenceFeatureForMappedVariantUnit:
    def test_sequence_feature_raises_when_target_is_missing(self, mock_mapped_variant):
        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=None):
            with pytest.raises(MappingDataDoesntExistException):
                sequence_feature_for_mapped_variant(mock_mapped_variant)

    def test_sequence_feature_returns_ensembl_identifier(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.util.extract_ids_from_post_mapped_metadata", return_value=["ENST00000357654"]
            ):
                feature, system = sequence_feature_for_mapped_variant(mock_mapped_variant)

        assert feature == "ENST00000357654"
        assert system == "https://www.ensembl.org/index.html"

    def test_sequence_feature_returns_refseq_identifier(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.util.extract_ids_from_post_mapped_metadata", return_value=["NM_000546.6"]
            ):
                feature, system = sequence_feature_for_mapped_variant(mock_mapped_variant)

        assert feature == "NM_000546.6"
        assert system == "https://www.ncbi.nlm.nih.gov/refseq/"

    def test_sequence_feature_returns_unknown_identifier_source(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.util.extract_ids_from_post_mapped_metadata", return_value=["CUSTOM_ID_1"]
            ):
                feature, system = sequence_feature_for_mapped_variant(mock_mapped_variant)

        assert feature == "CUSTOM_ID_1"
        assert system == "transcript or gene identifier of unknown source"

    def test_sequence_feature_falls_back_to_target_name(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={}, name="TP53")

        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=target):
            with patch("mavedb.lib.annotation.util.extract_ids_from_post_mapped_metadata", return_value=[]):
                feature, system = sequence_feature_for_mapped_variant(mock_mapped_variant)

        assert feature == "TP53"
        assert system == "https://www.mavedb.org/"

    def test_sequence_feature_raises_when_target_has_no_name_or_ids(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={}, name=None)

        with patch("mavedb.lib.annotation.util.target_for_variant", return_value=target):
            with patch("mavedb.lib.annotation.util.extract_ids_from_post_mapped_metadata", return_value=[]):
                with pytest.raises(MappingDataDoesntExistException):
                    sequence_feature_for_mapped_variant(mock_mapped_variant)


@pytest.mark.unit
class TestSerializeEvidenceItems:
    def test_serialize_evidence_items_serializes_all_items_in_order(self):
        first_item = SimpleNamespace(model_dump=lambda *, exclude_none: {"id": "first", "exclude_none": exclude_none})
        second_item = SimpleNamespace(model_dump=lambda *, exclude_none: {"id": "second", "exclude_none": exclude_none})

        result = serialize_evidence_items([first_item, second_item])

        assert result == [
            {"id": "first", "exclude_none": True},
            {"id": "second", "exclude_none": True},
        ]

    def test_serialize_evidence_items_raises_for_non_dumpable_item(self):
        with pytest.raises(TypeError, match="model_dump"):
            serialize_evidence_items([SimpleNamespace(not_model_dump=True)])


@pytest.mark.integration
class TestAnnotationUtilIntegration:
    """Integration tests for utility helpers using persisted DB-backed variants."""

    def test_variation_from_persisted_mapped_variant(self, setup_lib_db_with_mapped_variant):
        setup_lib_db_with_mapped_variant.post_mapped = TEST_VALID_POST_MAPPED_VRS_ALLELE
        variation = variation_from_mapped_variant(setup_lib_db_with_mapped_variant)

        assert variation is not None
        assert variation.model_dump().get("type") in {"Allele", "CisPhasedBlock"}

    def test_annotation_eligibility_returns_boolean_for_persisted_variant(self, setup_lib_db_with_mapped_variant):
        # Make score presence explicit so a negative result is due to missing calibrations.
        setup_lib_db_with_mapped_variant.variant.data = {"score_data": {"score": 1.0}}

        pathogenicity_allowed = can_annotate_variant_for_pathogenicity_evidence(setup_lib_db_with_mapped_variant)
        functional_allowed = can_annotate_variant_for_functional_statement(setup_lib_db_with_mapped_variant)

        # DB fixture score sets do not include calibrations by default, so both should be False.
        assert setup_lib_db_with_mapped_variant.variant.score_set.score_calibrations == []
        assert pathogenicity_allowed is False
        assert functional_allowed is False


@pytest.mark.unit
class TestSelectStrongestFunctionalCalibrationUnit:
    """Unit tests for select_strongest_functional_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_mapped_variant):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_functional_calibration(mock_mapped_variant, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that single calibration is returned."""
        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        calibrations = mapped_variant.variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_functional_calibration(mapped_variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_returns_first_when_all_agree(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that first calibration is returned when all have same classification."""
        from copy import deepcopy

        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        # Create multiple calibrations with same classification
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        calibration, functional_range = select_strongest_functional_calibration(mapped_variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]  # Should return the first one

    def test_defaults_to_normal_on_conflict(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that normal classification is preferred when there are conflicts."""
        from copy import deepcopy
        from unittest.mock import MagicMock, patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return different classifications
        with patch(
            "mavedb.lib.annotation.util.functional_classification_of_variant",
            side_effect=[
                (MagicMock(label="Abnormal"), ExperimentalVariantFunctionalImpactClassification.ABNORMAL),
                (MagicMock(label="Normal"), ExperimentalVariantFunctionalImpactClassification.NORMAL),
            ],
        ):
            calibration, functional_range = select_strongest_functional_calibration(mapped_variant, calibrations)

            # Should return the normal classification (second one)
            assert calibration == calibration2
            assert functional_range.label == "Normal"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        calibrations = mapped_variant.variant.score_set.score_calibrations

        # Mock to return None range but INDETERMINATE classification (variant not in any range)
        with patch(
            "mavedb.lib.annotation.util.functional_classification_of_variant",
            return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        ):
            calibration, functional_range = select_strongest_functional_calibration(mapped_variant, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None


@pytest.mark.unit
class TestSelectStrongestPathogenicityCalibrationUnit:
    """Unit tests for select_strongest_pathogenicity_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_mapped_variant):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_pathogenicity_calibration(mock_mapped_variant, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that single calibration is returned."""
        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibrations = mapped_variant.variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_selects_strongest_evidence_strength(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that calibration with strongest evidence is selected."""
        from copy import deepcopy
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return different evidence strengths
        with patch(
            "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
            side_effect=[
                (
                    MagicMock(label="Moderate"),
                    VariantPathogenicityEvidenceLine.Criterion.PS3,
                    StrengthOfEvidenceProvided.MODERATE,
                ),
                (
                    MagicMock(label="Strong"),
                    VariantPathogenicityEvidenceLine.Criterion.PS3,
                    StrengthOfEvidenceProvided.VERY_STRONG,
                ),
            ],
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

            # Should return the one with VERY_STRONG evidence
            assert calibration == calibration2
            assert functional_range.label == "Strong"

    def test_defaults_to_uncertain_on_tie(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that uncertain significance is returned when benign and pathogenic evidence tie."""
        from copy import deepcopy
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return same evidence strength but different criteria (pathogenic vs benign)
        with patch(
            "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
            side_effect=[
                (
                    MagicMock(label="Pathogenic"),
                    VariantPathogenicityEvidenceLine.Criterion.PS3,
                    StrengthOfEvidenceProvided.STRONG,
                ),
                (
                    MagicMock(label="Benign"),
                    VariantPathogenicityEvidenceLine.Criterion.BS3,
                    StrengthOfEvidenceProvided.STRONG,
                ),
            ],
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

            # Should return first calibration but None for range to indicate uncertain significance
            assert calibration == calibration1
            assert functional_range is None

    def test_returns_classification_when_all_tied_are_same_type(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that classification is returned normally when all tied candidates are the same type."""
        from copy import deepcopy
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return same evidence strength and same type of criteria (both benign)
        with patch(
            "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
            side_effect=[
                (
                    MagicMock(label="Benign1"),
                    VariantPathogenicityEvidenceLine.Criterion.BS3,
                    StrengthOfEvidenceProvided.STRONG,
                ),
                (
                    MagicMock(label="Benign2"),
                    VariantPathogenicityEvidenceLine.Criterion.BP1,
                    StrengthOfEvidenceProvided.STRONG,
                ),
            ],
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

            # Should return first calibration with its range (no conflict, so normal classification)
            assert calibration == calibration1
            assert functional_range.label == "Benign1"

    def test_handles_none_evidence_strength(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that None evidence strength is handled correctly."""
        from copy import deepcopy
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibration1 = mapped_variant.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock with None and actual strength
        with patch(
            "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
            side_effect=[
                (MagicMock(label="No Strength"), VariantPathogenicityEvidenceLine.Criterion.PS3, None),
                (
                    MagicMock(label="Moderate"),
                    VariantPathogenicityEvidenceLine.Criterion.PS3,
                    StrengthOfEvidenceProvided.MODERATE,
                ),
            ],
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

            # Should return the one with actual strength (second)
            assert calibration == calibration2
            assert functional_range.label == "Moderate"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        calibrations = mapped_variant.variant.score_set.score_calibrations

        # Mock to return None range for all calibrations (variant not in any range)
        with patch(
            "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
            return_value=(None, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(mapped_variant, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None
