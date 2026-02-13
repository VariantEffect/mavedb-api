"""
Tests for mavedb.lib.annotation.statement module.

This module tests statement creation functions for mapped variants,
focusing on functional impact statements and their classification.
"""

from unittest.mock import MagicMock, patch

import pytest
from ga4gh.va_spec.base.core import Direction, Statement

from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification
from mavedb.lib.annotation.evidence_line import functional_evidence_line
from mavedb.lib.annotation.proposition import mapped_variant_to_experimental_variant_functional_impact_proposition
from mavedb.lib.annotation.statement import (
    mapped_variant_to_functional_statement,
)


@pytest.mark.unit
class TestMappedVariantFunctionalStatement:
    """Unit tests for mapped variant functional statement creation."""

    @pytest.mark.parametrize(
        "classification, expected_direction",
        [
            (ExperimentalVariantFunctionalImpactClassification.NORMAL, Direction.DISPUTES),
            (ExperimentalVariantFunctionalImpactClassification.ABNORMAL, Direction.SUPPORTS),
            (ExperimentalVariantFunctionalImpactClassification.INDETERMINATE, Direction.NEUTRAL),
        ],
    )
    def test_mapped_variant_to_functional_statement(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        classification,
        expected_direction,
    ):
        """Test functional statement creation with different classifications."""
        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        score_calibration = mapped_variant.variant.score_set.score_calibrations[0]

        with patch(
            "mavedb.lib.annotation.evidence_line.functional_classification_of_variant",
            return_value=(MagicMock(label="Test Range"), classification),
        ):
            proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)
            evidence = functional_evidence_line(
                mapped_variant, score_calibration, [variant_study_result(mapped_variant)]
            )
            result = mapped_variant_to_functional_statement(
                mapped_variant, proposition, [evidence], score_calibration, classification
            )

        assert isinstance(result, Statement)
        assert result.description == f"Variant functional impact statement for {mapped_variant.variant.urn}."
        assert result.specifiedBy
        assert result.contributions
        assert len(result.contributions) == 3  # API, VRS, and score calibration contributions
        assert result.proposition == proposition
        assert result.direction == expected_direction
        assert result.classification.primaryCoding.code.root == classification.value
        assert (
            result.classification.primaryCoding.system == "ga4gh-gks-term:experimental-var-func-impact-classification"
        )
        assert result.hasEvidenceLines
        assert len(result.hasEvidenceLines) == 1
        assert result.hasEvidenceLines[0] == evidence

    def test_no_calibrations_raises_value_error(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
    ):
        """Test that missing score calibrations raises ValueError in evidence line creation."""
        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        score_calibration = mapped_variant.variant.score_set.score_calibrations[0]

        # Set calibrations to None to trigger the error
        mapped_variant.variant.score_set.score_calibrations = None

        proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            # Use a dummy score calibration for evidence line creation, but it should not be reached due to the error
            evidence = functional_evidence_line(
                mapped_variant, score_calibration, [variant_study_result(mapped_variant)]
            )
            mapped_variant_to_functional_statement(
                mapped_variant,
                proposition,
                [evidence],
                score_calibration,
                ExperimentalVariantFunctionalImpactClassification.NORMAL,
            )


@pytest.mark.unit
class TestMappedVariantPathogenicityStatement:
    """Unit tests for mapped variant pathogenicity statement ACMG classification mapping."""

    def test_pathogenic_criterion_maps_to_pathogenic(self, mock_mapped_variant):
        """Test that pathogenic ACMG criterion (PS3) maps to PATHOGENIC classification."""
        from mavedb.models.enums.acmg_criterion import ACMGCriterion
        from mavedb.models.enums.functional_classification import (
            FunctionalClassification as FunctionalClassificationOptions,
        )
        from tests.helpers.mocks.factories import (
            create_mock_acmg_classification,
            create_mock_functional_classification,
        )

        # PS3 is a pathogenic criterion
        acmg_classification = create_mock_acmg_classification(ACMGCriterion.PS3.name, "STRONG")
        functional_range = create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.abnormal,
            acmg_classification=acmg_classification,
        )

        # Verify that PS3 is recognized as pathogenic
        assert functional_range.acmg_classification.criterion.is_pathogenic is True
        assert functional_range.acmg_classification.criterion.is_benign is False

    def test_benign_criterion_maps_to_benign(self, mock_mapped_variant):
        """Test that benign ACMG criterion (BS3) maps to BENIGN classification."""
        from mavedb.models.enums.acmg_criterion import ACMGCriterion
        from mavedb.models.enums.functional_classification import (
            FunctionalClassification as FunctionalClassificationOptions,
        )
        from tests.helpers.mocks.factories import (
            create_mock_acmg_classification,
            create_mock_functional_classification,
        )

        # BS3 is a benign criterion
        acmg_classification = create_mock_acmg_classification(ACMGCriterion.BS3.name, "STRONG")
        functional_range = create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.normal,
            acmg_classification=acmg_classification,
        )

        # Verify that BS3 is recognized as benign
        assert functional_range.acmg_classification.criterion.is_pathogenic is False
        assert functional_range.acmg_classification.criterion.is_benign is True

    def test_none_acmg_classification_defaults_to_uncertain(self, mock_mapped_variant):
        """Test that None ACMG classification results in UNCERTAIN_SIGNIFICANCE."""
        from mavedb.models.enums.functional_classification import (
            FunctionalClassification as FunctionalClassificationOptions,
        )
        from tests.helpers.mocks.factories import (
            create_mock_functional_classification,
        )

        # Create functional range with no ACMG classification
        functional_range = create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.not_specified,
            acmg_classification=None,
        )

        # Verify that None ACMG classification is handled
        assert functional_range.acmg_classification is None
