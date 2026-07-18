# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.calibration — calibration eligibility + strongest-evidence selection."""

from copy import deepcopy

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.calibration import (
    score_calibration_may_be_used_for_annotation,
    select_strongest_functional_calibration,
    select_strongest_pathogenicity_calibration,
)


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
class TestSelectStrongestFunctionalCalibrationUnit:
    """Unit tests for select_strongest_functional_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_mapped_variant):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_functional_calibration(mock_mapped_variant.variant, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that single calibration is returned."""
        variant = mock_mapped_variant_with_functional_calibration_score_set.variant
        calibrations = variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_functional_calibration(variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_returns_first_when_all_agree(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that first calibration is returned when all have same classification."""
        variant = mock_mapped_variant_with_functional_calibration_score_set.variant
        # Create multiple calibrations with same classification
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        calibration, functional_range = select_strongest_functional_calibration(variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]  # Should return the first one

    def test_defaults_to_normal_on_conflict(self, mock_mapped_variant_with_functional_calibration_score_set):
        """Test that normal classification is preferred when there are conflicts."""
        from unittest.mock import MagicMock, patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        variant = mock_mapped_variant_with_functional_calibration_score_set.variant
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return different classifications
        with patch(
            "mavedb.lib.annotation.calibration.functional_classification_of_variant",
            side_effect=[
                (MagicMock(label="Abnormal"), ExperimentalVariantFunctionalImpactClassification.ABNORMAL),
                (MagicMock(label="Normal"), ExperimentalVariantFunctionalImpactClassification.NORMAL),
            ],
        ):
            calibration, functional_range = select_strongest_functional_calibration(variant, calibrations)

            # Should return the normal classification (second one)
            assert calibration == calibration2
            assert functional_range.label == "Normal"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_mapped_variant_with_functional_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        variant = mock_mapped_variant_with_functional_calibration_score_set.variant
        calibrations = variant.score_set.score_calibrations

        # Mock to return None range but INDETERMINATE classification (variant not in any range)
        with patch(
            "mavedb.lib.annotation.calibration.functional_classification_of_variant",
            return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        ):
            calibration, functional_range = select_strongest_functional_calibration(variant, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None


@pytest.mark.unit
class TestSelectStrongestPathogenicityCalibrationUnit:
    """Unit tests for select_strongest_pathogenicity_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_mapped_variant):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_pathogenicity_calibration(mock_mapped_variant.variant, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that single calibration is returned."""
        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibrations = variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_selects_strongest_evidence_strength(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that calibration with strongest evidence is selected."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return different evidence strengths
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

            # Should return the one with VERY_STRONG evidence
            assert calibration == calibration2
            assert functional_range.label == "Strong"

    def test_defaults_to_uncertain_on_tie(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that uncertain significance is returned when benign and pathogenic evidence tie."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return same evidence strength but different criteria (pathogenic vs benign)
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

            # Should return first calibration but None for range to indicate uncertain significance
            assert calibration == calibration1
            assert functional_range is None

    def test_returns_classification_when_all_tied_are_same_type(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that classification is returned normally when all tied candidates are the same type."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock to return same evidence strength and same type of criteria (both benign)
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

            # Should return first calibration with its range (no conflict, so normal classification)
            assert calibration == calibration1
            assert functional_range.label == "Benign1"

    def test_handles_none_evidence_strength(self, mock_mapped_variant_with_pathogenicity_calibration_score_set):
        """Test that None evidence strength is handled correctly."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibration1 = variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        # Mock with None and actual strength
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
            side_effect=[
                (MagicMock(label="No Strength"), VariantPathogenicityEvidenceLine.Criterion.PS3, None),
                (
                    MagicMock(label="Moderate"),
                    VariantPathogenicityEvidenceLine.Criterion.PS3,
                    StrengthOfEvidenceProvided.MODERATE,
                ),
            ],
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

            # Should return the one with actual strength (second)
            assert calibration == calibration2
            assert functional_range.label == "Moderate"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_mapped_variant_with_pathogenicity_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

        variant = mock_mapped_variant_with_pathogenicity_calibration_score_set.variant
        calibrations = variant.score_set.score_calibrations

        # Mock to return None range for all calibrations (variant not in any range)
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
            return_value=(None, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(variant, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None
