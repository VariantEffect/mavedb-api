# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.classification module.

This module tests variant classification functions for functional and pathogenicity classification,
ensuring proper handling of score calibrations, functional ranges, and ACMG criteria.
"""

from enum import StrEnum
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg2")

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided as GA4GHStrengthOfEvidenceProvided

from mavedb.lib.annotation.classification import (
    ExperimentalVariantFunctionalImpactClassification,
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from tests.helpers.mocks.factories import (
    create_mock_acmg_classification,
    create_mock_functional_classification,
    create_mock_mapped_variant,
    create_mock_pathogenicity_range,
    create_mock_score_calibration,
)


@pytest.mark.unit
class TestExperimentalVariantFunctionalImpactClassificationEnum:
    """Test the ExperimentalVariantFunctionalImpactClassification enum."""

    def test_enum_values(self):
        """Test that enum has expected values."""
        assert ExperimentalVariantFunctionalImpactClassification.NORMAL == "normal"
        assert ExperimentalVariantFunctionalImpactClassification.ABNORMAL == "abnormal"
        assert ExperimentalVariantFunctionalImpactClassification.INDETERMINATE == "indeterminate"

    def test_enum_is_str_enum(self):
        """Test that enum inherits from StrEnum."""
        assert issubclass(ExperimentalVariantFunctionalImpactClassification, StrEnum)

    def test_enum_members_count(self):
        """Test that enum has exactly 3 members."""
        assert len(ExperimentalVariantFunctionalImpactClassification) == 3


@pytest.mark.unit
class TestFunctionalClassificationOfVariantUnit:
    """Unit tests for functional_classification_of_variant function."""

    def test_normal_classification(self):
        """Test variant classified as normal."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        functional_range = create_mock_functional_classification(FunctionalClassificationOptions.normal)
        score_calibration = create_mock_score_calibration(functional_classifications=[functional_range])

        result = functional_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == functional_range
        assert result[1] == ExperimentalVariantFunctionalImpactClassification.NORMAL

    def test_abnormal_classification(self):
        """Test variant classified as abnormal."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        functional_range = create_mock_functional_classification(FunctionalClassificationOptions.abnormal)
        score_calibration = create_mock_score_calibration(functional_classifications=[functional_range])

        result = functional_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == functional_range
        assert result[1] == ExperimentalVariantFunctionalImpactClassification.ABNORMAL

    def test_indeterminate_classification(self):
        """Test variant classified as indeterminate."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        functional_range = create_mock_functional_classification(FunctionalClassificationOptions.not_specified)
        score_calibration = create_mock_score_calibration(functional_classifications=[functional_range])

        result = functional_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == functional_range
        assert result[1] == ExperimentalVariantFunctionalImpactClassification.INDETERMINATE

    def test_variant_not_in_any_range(self):
        """Test variant not found in any functional range returns None and INDETERMINATE."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        functional_range = create_mock_functional_classification(
            FunctionalClassificationOptions.normal, variant_in_range=False
        )
        score_calibration = create_mock_score_calibration(functional_classifications=[functional_range])

        result = functional_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] is None
        assert result[1] == ExperimentalVariantFunctionalImpactClassification.INDETERMINATE

    def test_multiple_ranges_returns_first_match(self):
        """
        Test that function returns first matching range.
        In practice, this should not occur because ranges should be mutually exclusive,
        but test for expected behavior.
        """
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        first_range = create_mock_functional_classification(FunctionalClassificationOptions.normal)
        second_range = create_mock_functional_classification(FunctionalClassificationOptions.abnormal)
        score_calibration = create_mock_score_calibration(functional_classifications=[first_range, second_range])

        result = functional_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == first_range
        assert result[1] == ExperimentalVariantFunctionalImpactClassification.NORMAL

    def test_missing_score_calibrations_raises_error(self):
        """Test that missing score calibrations raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = None
        score_calibration = create_mock_score_calibration()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            functional_classification_of_variant(mapped_variant, score_calibration)

    def test_empty_score_calibrations_raises_error(self):
        """Test that empty score calibrations list raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = []
        score_calibration = create_mock_score_calibration()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            functional_classification_of_variant(mapped_variant, score_calibration)

    def test_missing_functional_classifications_raises_error(self):
        """Test that missing functional classifications raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]
        score_calibration = create_mock_score_calibration(functional_classifications=[])

        with pytest.raises(ValueError, match="does not have ranges defined in its primary score calibration"):
            functional_classification_of_variant(mapped_variant, score_calibration)


@pytest.mark.unit
class TestPathogenicityClassificationOfVariantUnit:
    """Unit tests for pathogenicity_classification_of_variant function."""

    def test_valid_pathogenicity_classification(self):
        """Test variant with valid ACMG classification."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        acmg_classification = create_mock_acmg_classification("PS3", "STRONG")
        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == pathogenicity_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] == GA4GHStrengthOfEvidenceProvided.STRONG

    def test_moderate_plus_maps_to_moderate(self):
        """Test that MODERATE_PLUS evidence strength maps to MODERATE for VA-Spec compatibility."""
        from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided as MaveDBStrength

        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        # Create ACMG classification with MODERATE_PLUS strength
        acmg_classification = create_mock_acmg_classification("PS3", MaveDBStrength.MODERATE_PLUS.name)
        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        # Verify the mapped evidence strength is MODERATE, not MODERATE_PLUS
        assert result[0] == pathogenicity_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] == GA4GHStrengthOfEvidenceProvided.MODERATE

    def test_none_acmg_classification(self):
        """Test variant with None ACMG classification returns PS3 and None strength."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification=None)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == pathogenicity_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] is None

    def test_variant_not_in_any_range(self):
        """Test variant not found in any pathogenicity range."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        acmg_classification = create_mock_acmg_classification()
        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification, variant_in_range=False)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] is None
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] is None

    def test_missing_score_calibrations_raises_error(self):
        """Test that missing score calibrations raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = None
        score_calibration = create_mock_score_calibration()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            pathogenicity_classification_of_variant(mapped_variant, score_calibration)

    def test_empty_score_calibrations_raises_error(self):
        """Test that empty score calibrations list raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = []
        score_calibration = create_mock_score_calibration()

        with pytest.raises(ValueError, match="does not have a score set with score calibrations"):
            pathogenicity_classification_of_variant(mapped_variant, score_calibration)

    def test_missing_functional_classifications_raises_error(self):
        """Test that missing functional classifications raises ValueError."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]
        score_calibration = create_mock_score_calibration(functional_classifications=[])

        with pytest.raises(ValueError, match="does not have ranges defined in its primary score calibration"):
            pathogenicity_classification_of_variant(mapped_variant, score_calibration)

    def test_moderate_plus_evidence_strength_maps_to_moderate(self):
        """Test that MODERATE_PLUS evidence strength maps to Moderate."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        acmg_classification = create_mock_acmg_classification("PS3", "MODERATE_PLUS")
        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == pathogenicity_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] == GA4GHStrengthOfEvidenceProvided.MODERATE

    @pytest.mark.parametrize(
        "criterion,strength",
        [
            ("PS3", "STRONG"),
            ("PM2", "MODERATE"),
            ("PP3", "SUPPORTING"),
            ("BS3", "STRONG"),
        ],
    )
    def test_various_valid_acmg_combinations(self, criterion, strength):
        """Test various valid ACMG criterion and strength combinations."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        acmg_classification = create_mock_acmg_classification(criterion, strength)
        pathogenicity_range = create_mock_pathogenicity_range(acmg_classification)
        score_calibration = create_mock_score_calibration(functional_classifications=[pathogenicity_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == pathogenicity_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion[criterion]
        assert result[2] == GA4GHStrengthOfEvidenceProvided[strength]

    def test_multiple_ranges_returns_first_match(self):
        """Test that function returns first matching pathogenicity range."""
        mapped_variant = create_mock_mapped_variant()
        mapped_variant.variant.score_set.score_calibrations = [MagicMock()]

        first_acmg = create_mock_acmg_classification("PS3", "STRONG")
        first_range = create_mock_pathogenicity_range(first_acmg)

        second_acmg = create_mock_acmg_classification("PM2", "MODERATE")
        second_range = create_mock_pathogenicity_range(second_acmg)

        score_calibration = create_mock_score_calibration(functional_classifications=[first_range, second_range])

        result = pathogenicity_classification_of_variant(mapped_variant, score_calibration)

        assert result[0] == first_range
        assert result[1] == VariantPathogenicityEvidenceLine.Criterion.PS3
        assert result[2] == GA4GHStrengthOfEvidenceProvided.STRONG
