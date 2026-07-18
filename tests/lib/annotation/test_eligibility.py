# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.eligibility — variant annotatability predicates."""

from copy import deepcopy
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.eligibility import (
    _can_annotate_variant_base_assumptions,
    _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
)


@pytest.mark.unit
class TestBaseAnnotationAssumptionsUnit:
    def test_base_assumption_check_returns_false_when_score_is_none(self, mock_mapped_variant):
        mock_mapped_variant.variant.data = {"score_data": {"score": None}}

        assert _can_annotate_variant_base_assumptions(mock_mapped_variant.variant) is False

    def test_base_assumption_check_returns_true_when_all_conditions_met(self, mock_mapped_variant):
        assert _can_annotate_variant_base_assumptions(mock_mapped_variant.variant) is True


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
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant.variant, kind
            )
            is False
        )

    @pytest.mark.parametrize("kind", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_no_calibrations_present(self, mock_mapped_variant, kind):
        mock_mapped_variant.variant.score_set.score_calibrations = []
        assert (
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant.variant, kind
            )
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
                mock_mapped_variant_with_functional_calibration_score_set.variant, annotation_type
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
                mock_mapped_variant.variant, kind, allow_research_use_only_calibrations=True
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
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant.variant, kind
            )
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
                mock_mapped_variant_with_pathogenicity_calibration_score_set.variant, "pathogenicity"
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
                mock_mapped_variant_with_pathogenicity_calibration_score_set.variant, "pathogenicity"
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
            _variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation(
                mock_mapped_variant.variant, kind
            )
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
                mock_mapped_variant_with_functional_calibration_score_set.variant, "functional"
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
                mock_mapped_variant_with_pathogenicity_calibration_score_set.variant, "pathogenicity"
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
                mock_mapped_variant_with_functional_calibration_score_set.variant, "functional"
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
                mock_mapped_variant_with_functional_calibration_score_set.variant, "pathogenicity"
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
                mock_mapped_variant_with_functional_calibration_score_set.variant, "functional"
            )
            is False
        )


@pytest.mark.unit
class TestPathogenicityAnnotationEligibilityUnit:
    def test_pathogenicity_range_check_returns_false_when_base_assumptions_fail(self, mock_mapped_variant):
        with patch("mavedb.lib.annotation.eligibility._can_annotate_variant_base_assumptions", return_value=False):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant.variant)

        assert result is False

    def test_pathogenicity_range_check_returns_false_when_pathogenicity_ranges_check_fails(self, mock_mapped_variant):
        with patch(
            "mavedb.lib.annotation.eligibility._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
            return_value=False,
        ):
            result = can_annotate_variant_for_pathogenicity_evidence(mock_mapped_variant.variant)

        assert result is False

    def test_pathogenicity_range_check_returns_true_when_all_conditions_met(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ):
        assert (
            can_annotate_variant_for_pathogenicity_evidence(
                mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
            )
            is True
        )


@pytest.mark.unit
class TestFunctionalAnnotationEligibilityUnit:
    def test_functional_range_check_returns_false_when_base_assumptions_fail(self, mock_mapped_variant):
        with patch(
            "mavedb.lib.annotation.eligibility._can_annotate_variant_base_assumptions",
            return_value=False,
        ):
            result = can_annotate_variant_for_functional_statement(mock_mapped_variant.variant)

        assert result is False

    def test_functional_range_check_returns_false_when_functional_classifications_check_fails(
        self, mock_mapped_variant
    ):
        with patch(
            "mavedb.lib.annotation.eligibility._variant_score_calibrations_have_required_calibrations_and_ranges_for_annotation",
            return_value=False,
        ):
            result = can_annotate_variant_for_functional_statement(mock_mapped_variant.variant)

        assert result is False

    def test_functional_range_check_returns_true_when_all_conditions_met(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        assert (
            can_annotate_variant_for_functional_statement(
                mock_mapped_variant_with_functional_calibration_score_set.variant
            )
            is True
        )


@pytest.mark.integration
class TestAnnotationEligibilityIntegration:
    def test_annotation_eligibility_returns_boolean_for_persisted_variant(self, setup_lib_db_with_mapped_variant):
        # Make score presence explicit so a negative result is due to missing calibrations.
        setup_lib_db_with_mapped_variant.variant.data = {"score_data": {"score": 1.0}}

        pathogenicity_allowed = can_annotate_variant_for_pathogenicity_evidence(
            setup_lib_db_with_mapped_variant.variant
        )
        functional_allowed = can_annotate_variant_for_functional_statement(setup_lib_db_with_mapped_variant.variant)

        # DB fixture score sets do not include calibrations by default, so both should be False.
        assert setup_lib_db_with_mapped_variant.variant.score_set.score_calibrations == []
        assert pathogenicity_allowed is False
        assert functional_allowed is False
