from unittest.mock import Mock

import pytest
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import Direction, EvidenceLine

from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification
from mavedb.lib.annotation.direction import (
    aggregate_direction_of_evidence,
    direction_of_support_for_functional_classification,
    direction_of_support_for_pathogenicity_classification,
)


@pytest.mark.unit
class TestAggregateDirectionOfEvidenceUnit:
    """Unit tests for aggregate_direction_of_evidence function."""

    def test_all_supports_returns_supports(self):
        """Test that all supporting evidence returns SUPPORTS."""
        evidence = [
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
        ]

        result = aggregate_direction_of_evidence(evidence)
        assert result == Direction.SUPPORTS

    def test_all_disputes_returns_disputes(self):
        """Test that all disputing evidence returns DISPUTES."""
        evidence = [
            Mock(directionOfEvidenceProvided=Direction.DISPUTES),
            Mock(directionOfEvidenceProvided=Direction.DISPUTES),
        ]

        result = aggregate_direction_of_evidence(evidence)
        assert result == Direction.DISPUTES

    def test_mixed_evidence_returns_neutral(self):
        """Test that mixed evidence returns NEUTRAL."""
        evidence = [
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
            Mock(directionOfEvidenceProvided=Direction.DISPUTES),
        ]

        result = aggregate_direction_of_evidence(evidence)
        assert result == Direction.NEUTRAL

    def test_any_neutral_evidence_returns_neutral(self):
        """Test that any neutral evidence makes the result NEUTRAL."""
        evidence = [
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
            Mock(directionOfEvidenceProvided=Direction.NEUTRAL),
            Mock(directionOfEvidenceProvided=Direction.SUPPORTS),
        ]

        result = aggregate_direction_of_evidence(evidence)
        assert result == Direction.NEUTRAL

    def test_empty_evidence_list_returns_neutral(self):
        """Test that empty evidence list returns NEUTRAL."""
        result = aggregate_direction_of_evidence([])
        assert result == Direction.NEUTRAL

    def test_single_evidence_item_returns_its_direction(self):
        """Test that single evidence item returns its own direction."""
        test_cases = [
            (Direction.SUPPORTS, Direction.SUPPORTS),
            (Direction.DISPUTES, Direction.DISPUTES),
            (Direction.NEUTRAL, Direction.NEUTRAL),
        ]

        for input_direction, expected_direction in test_cases:
            evidence = [Mock(directionOfEvidenceProvided=input_direction)]
            result = aggregate_direction_of_evidence(evidence)
            assert result == expected_direction

    def test_handles_real_evidence_line_objects(self):
        """Test that function works with actual EvidenceLine objects."""
        # Create mock evidence lines with the expected attribute
        evidence_line_1 = Mock(spec=EvidenceLine)
        evidence_line_1.directionOfEvidenceProvided = Direction.SUPPORTS

        evidence_line_2 = Mock(spec=EvidenceLine)
        evidence_line_2.directionOfEvidenceProvided = Direction.SUPPORTS

        result = aggregate_direction_of_evidence([evidence_line_1, evidence_line_2])
        assert result == Direction.SUPPORTS


@pytest.mark.unit
class TestDirectionOfSupportForFunctionalClassificationUnit:
    """Unit tests for direction_of_support_for_functional_classification function."""

    @pytest.mark.parametrize(
        "classification,expected_direction",
        [
            (ExperimentalVariantFunctionalImpactClassification.NORMAL, Direction.DISPUTES),
            (ExperimentalVariantFunctionalImpactClassification.ABNORMAL, Direction.SUPPORTS),
            (ExperimentalVariantFunctionalImpactClassification.INDETERMINATE, Direction.NEUTRAL),
        ],
    )
    def test_maps_functional_classifications_to_correct_directions(self, classification, expected_direction):
        """Test that functional classifications map to expected directions."""
        result = direction_of_support_for_functional_classification(classification)
        assert result == expected_direction

    def test_handles_all_classification_enum_values(self):
        """Test that all enum values are handled without errors."""
        for classification in ExperimentalVariantFunctionalImpactClassification:
            result = direction_of_support_for_functional_classification(classification)
            assert isinstance(result, Direction)


@pytest.mark.unit
class TestDirectionOfSupportForPathogenicityClassificationUnit:
    """Unit tests for direction_of_support_for_pathogenicity_classification function."""

    @pytest.mark.parametrize(
        "criterion,expected_direction",
        [
            (VariantPathogenicityEvidenceLine.Criterion.PS3, Direction.SUPPORTS),
            (VariantPathogenicityEvidenceLine.Criterion.BS3, Direction.DISPUTES),
            (None, Direction.NEUTRAL),
        ],
    )
    def test_maps_acmg_criteria_to_correct_directions(self, criterion, expected_direction):
        """Test that ACMG criteria map to expected directions."""
        result = direction_of_support_for_pathogenicity_classification(criterion)
        assert result == expected_direction

    def test_raises_error_for_unsupported_criteria(self):
        """Test that unsupported ACMG criteria raise ValueError."""
        # Test with a criterion that should not be supported
        unsupported_criterion = VariantPathogenicityEvidenceLine.Criterion.PP1

        with pytest.raises(ValueError, match="Unsupported ACMG criterion"):
            direction_of_support_for_pathogenicity_classification(unsupported_criterion)

    def test_handles_none_criterion(self):
        """Test that None criterion returns NEUTRAL direction."""
        result = direction_of_support_for_pathogenicity_classification(None)
        assert result == Direction.NEUTRAL
