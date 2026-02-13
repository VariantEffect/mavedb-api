"""
Tests for mavedb.lib.annotation.evidence_line module.

This module tests evidence line creation functions for ACMG and functional
evidence lines, including pathogenicity classification and strength handling.
"""

from unittest.mock import MagicMock, patch

import pytest
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import Direction, EvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import mapped_variant_to_experimental_variant_clinical_impact_proposition


@pytest.mark.unit
class TestAcmgEvidenceLine:
    """Unit tests for ACMG evidence line creation."""

    @pytest.mark.parametrize(
        "expected_outcome, expected_direction",
        [
            (VariantPathogenicityEvidenceLine.Criterion.BS3, Direction.DISPUTES),
            (VariantPathogenicityEvidenceLine.Criterion.PS3, Direction.SUPPORTS),
        ],
    )
    @pytest.mark.parametrize(
        "expected_strength",
        [
            StrengthOfEvidenceProvided.SUPPORTING,
            StrengthOfEvidenceProvided.MODERATE,
            StrengthOfEvidenceProvided.STRONG,
            StrengthOfEvidenceProvided.VERY_STRONG,
        ],
    )
    def test_acmg_evidence_line_with_met_valid_clinical_classification(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
        expected_outcome,
        expected_strength,
        expected_direction,
    ):
        """Test ACMG evidence line creation with met valid clinical classification."""
        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        score_calibration = mapped_variant.variant.score_set.score_calibrations[0]

        with patch(
            "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
            return_value=(MagicMock(label="Test Range"), expected_outcome, expected_strength),
        ):
            proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mapped_variant)
            study_result = variant_study_result(mapped_variant)
            result = acmg_evidence_line(mapped_variant, score_calibration, proposition, [study_result])

        if expected_strength == StrengthOfEvidenceProvided.STRONG:
            expected_evidence_outcome = expected_outcome.value
        else:
            expected_evidence_outcome = f"{expected_outcome.value}_{expected_strength.name.lower()}"

        assert isinstance(result, VariantPathogenicityEvidenceLine)
        assert result.description == f"Pathogenicity evidence line for {mapped_variant.variant.urn}."
        assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
        assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Met"
        assert result.strengthOfEvidenceProvided.primaryCoding.code.root == expected_strength
        assert result.strengthOfEvidenceProvided.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.directionOfEvidenceProvided == expected_direction if expected_strength else None
        assert result.contributions
        assert result.specifiedBy
        assert result.targetProposition == proposition
        assert len(result.hasEvidenceItems) == 1

    def test_acmg_evidence_line_with_not_met_clinical_classification(
        self,
        mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ):
        """Test ACMG evidence line creation with not met clinical classification."""
        mapped_variant = mock_mapped_variant_with_pathogenicity_calibration_score_set
        score_calibration = mapped_variant.variant.score_set.score_calibrations[0]
        expected_outcome = VariantPathogenicityEvidenceLine.Criterion.PS3
        expected_strength = None
        expected_evidence_outcome = f"{expected_outcome.value}_not_met"

        with patch(
            "mavedb.lib.annotation.evidence_line.pathogenicity_classification_of_variant",
            return_value=(MagicMock(label="Test Range"), expected_outcome, expected_strength),
        ):
            proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mapped_variant)
            study_result = variant_study_result(mapped_variant)
            result = acmg_evidence_line(mapped_variant, score_calibration, proposition, [study_result])

        assert isinstance(result, VariantPathogenicityEvidenceLine)
        assert result.description == f"Pathogenicity evidence line for {mapped_variant.variant.urn}."
        assert result.evidenceOutcome.primaryCoding.code.root == expected_evidence_outcome
        assert result.evidenceOutcome.primaryCoding.system == "ACMG Guidelines, 2015"
        assert result.evidenceOutcome.name == f"ACMG 2015 {expected_outcome.name} Criterion Not Met"
        assert result.strengthOfEvidenceProvided is None
        assert result.directionOfEvidenceProvided == Direction.NEUTRAL
        assert result.contributions
        assert result.specifiedBy
        assert result.targetProposition == proposition
        assert len(result.hasEvidenceItems) == 1

    def test_acmg_evidence_line_with_no_calibrations_raises_error(self, mock_mapped_variant):
        """Test that ACMG evidence line creation raises error when no calibrations exist."""
        mock_mapped_variant.variant.score_set.score_calibrations = None
        score_calibration = MagicMock()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)
            study_result = variant_study_result(mock_mapped_variant)
            acmg_evidence_line(mock_mapped_variant, score_calibration, proposition, [study_result])


@pytest.mark.unit
class TestFunctionalEvidenceLine:
    """Unit tests for functional evidence line creation."""

    def test_functional_evidence_line_with_valid_functional_evidence(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test functional evidence line creation with valid evidence."""
        mapped_variant = mock_mapped_variant_with_functional_calibration_score_set
        score_calibration = mapped_variant.variant.score_set.score_calibrations[0]
        study_result = variant_study_result(mapped_variant)
        result = functional_evidence_line(mapped_variant, score_calibration, [study_result])

        assert isinstance(result, EvidenceLine)
        assert result.description == f"Functional evidence line for {mapped_variant.variant.urn}"
        assert result.directionOfEvidenceProvided is not None
        assert result.specifiedBy
        assert result.contributions
        assert result.reportedIn
        assert len(result.hasEvidenceItems) == 1
