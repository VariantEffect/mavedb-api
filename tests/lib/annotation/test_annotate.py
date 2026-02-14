"""
Tests for mavedb.lib.annotation.annotate module.

This module tests the main annotation functions that create statements and study results
for variants, focusing on object structure and validation.
"""

# ruff: noqa: E402

from copy import deepcopy

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_statement,
    variant_study_result,
)


@pytest.mark.unit
class TestVariantStudyResult:
    """Unit tests for variant study result creation."""

    def test_variant_study_result_creates_valid_result(self, mock_mapped_variant):
        """Test that variant study result creates a valid result object."""
        result = variant_study_result(mock_mapped_variant)

        assert result is not None
        assert result.type == "ExperimentalVariantFunctionalImpactStudyResult"


@pytest.mark.unit
class TestVariantFunctionalImpactStatement:
    """Unit tests for variant functional impact statement creation."""

    def test_no_calibrations_returns_none(self, mock_mapped_variant):
        """Test that statement returns None when no calibrations exist."""
        result = variant_functional_impact_statement(mock_mapped_variant)

        assert result is None

    def test_only_research_use_only_calibrations_returns_none(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that statement returns None when only research use only primary calibrations exist."""
        # Set all calibrations to research use only
        for (
            calibration
        ) in mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)
        assert result is None

    def test_no_score_returns_none(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that statement returns None when variant has no score."""
        mock_mapped_variant_with_functional_calibration_score_set.variant.data = {"score_data": {"score": None}}
        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is None

    def test_valid_statement_creation(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test creating valid functional impact statement with proper structure."""
        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert result.type == "Statement"
        assert all(evidence_item.type == "EvidenceLine" for evidence_item in result.hasEvidenceLines)
        assert all(
            study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
            for evidence_line in [evidence_line for evidence_line in result.hasEvidenceLines]
            for study_result in evidence_line.hasEvidenceItems
        )

    def test_skips_research_use_only_calibrations_when_mixed(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that research-use-only calibrations are skipped when mixed with regular calibrations."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]
        mixed_calibrations[0].research_use_only = True
        mixed_calibrations[1].research_use_only = False
        mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_variant_not_in_any_range_returns_indeterminate(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that variant not in any functional range gets INDETERMINATE classification."""
        from unittest.mock import patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set

        # Mock functional_classification_of_variant to return None range (variant not in any range)
        with patch(
            "mavedb.lib.annotation.annotate.functional_classification_of_variant",
            return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        ):
            result = variant_functional_impact_statement(mapped_variant)

            assert result is not None
            assert result.type == "Statement"
            # Classification should be INDETERMINATE
            assert result.classification.primaryCoding.code.root == "indeterminate"


@pytest.mark.unit
class TestVariantPathogenicityStatement:
    """Unit tests for variant pathogenicity statement creation."""

    def test_no_calibrations_returns_none(self, mock_mapped_variant):
        """Test that statement returns None when no calibrations exist."""
        result = variant_pathogenicity_statement(mock_mapped_variant)

        assert result is None

    def test_no_score_returns_none(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that statement returns None when variant has no score."""
        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.data = {"score_data": {"score": None}}
        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is None

    def test_only_research_use_only_calibration_returns_none(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that statement returns None when only research use only primary calibrations exist."""
        # Set all calibrations to research use only
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        assert result is None

    def test_no_acmg_classifications_returns_none(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that statement returns None when no ACMG classifications exist."""
        # Remove ACMG classifications from all calibrations
        for (
            calibration
        ) in mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_removed = [deepcopy(r) for r in calibration.functional_classifications]
            for functional_classification in acmg_removed:
                functional_classification["acmgClassification"] = None
            calibration.functional_classifications = acmg_removed

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)
        assert result is None

    def test_valid_pathogenicity_statement_creation(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test creating valid pathogenicity statement with proper structure."""
        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert result.proposition.type == "VariantPathogenicityProposition"
        assert result.type == "Statement"

        pathogenicity_evidence_lines = [evidence_item for evidence_item in result.hasEvidenceLines]
        statements = [
            statement for evidence_item in pathogenicity_evidence_lines for statement in evidence_item.hasEvidenceItems
        ]
        functional_evidence_lines = [
            evidence_item for statement in statements for evidence_item in statement.hasEvidenceLines
        ]

        assert all(ei.type == "EvidenceLine" for ei in pathogenicity_evidence_lines)
        assert all(s.type == "Statement" for s in statements)
        assert all(ei.type == "EvidenceLine" for ei in functional_evidence_lines)
        assert all(
            study_result.root.type == "ExperimentalVariantFunctionalImpactStudyResult"
            for evidence_item in functional_evidence_lines
            for study_result in evidence_item.hasEvidenceItems
        )

    def test_skips_research_use_only_calibrations_when_mixed(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that research-use-only pathogenicity calibrations are skipped when mixed with regular calibrations."""
        calibrations = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]
        mixed_calibrations[0].research_use_only = True
        mixed_calibrations[1].research_use_only = False
        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_skips_invalid_calibrations_when_functional_annotation(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that functional annotation skips calibrations invalid under score_calibration_may_be_used_for_annotation."""
        calibrations = mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]

        # Invalid: no functional classifications
        mixed_calibrations[0].functional_classifications = []
        # Valid: retain default functional classifications
        mixed_calibrations[1].functional_classifications = deepcopy(calibrations[0].functional_classifications)

        mock_mapped_variant_with_functional_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_functional_impact_statement(mock_mapped_variant_with_functional_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_skips_invalid_calibrations_when_pathogenicity_annotation(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that pathogenicity annotation skips calibrations invalid under score_calibration_may_be_used_for_annotation."""
        calibrations = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
        mixed_calibrations = [deepcopy(calibrations[0]), deepcopy(calibrations[0])]

        # Invalid calibration: no functional classifications
        mixed_calibrations[0].functional_classifications = []

        # Valid calibration retained
        mixed_calibrations[1].functional_classifications = deepcopy(calibrations[0].functional_classifications)

        mock_mapped_variant_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations = (
            mixed_calibrations
        )

        result = variant_pathogenicity_statement(mock_mapped_variant_with_pathogenicity_calibration_score_set)

        assert result is not None
        assert len(result.hasEvidenceLines) == 1

    def test_variant_not_in_any_range_returns_uncertain_significance(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that variant not in any range gets UNCERTAIN_SIGNIFICANCE classification."""
        from unittest.mock import patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set

        # Mock both classification functions to return None range (variant not in any range)
        with (
            patch(
                "mavedb.lib.annotation.annotate.functional_classification_of_variant",
                return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
            ),
            patch(
                "mavedb.lib.annotation.util.pathogenicity_classification_of_variant",
                return_value=(None, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
            ),
        ):
            result = variant_pathogenicity_statement(mapped_variant)

            assert result is not None
            assert result.type == "Statement"
            # Classification should be UNCERTAIN_SIGNIFICANCE
            assert result.classification.primaryCoding.code.root == "uncertain significance"
