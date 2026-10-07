# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.calibration — calibration eligibility + strongest-evidence selection."""

from copy import deepcopy

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.calibration import (
    calibrations_available_for_annotation,
    score_calibration_may_be_used_for_annotation,
    select_strongest_functional_calibration,
    select_strongest_pathogenicity_calibration,
)
from mavedb.lib.permissions.principal import Principal
from tests.lib.annotation.conftest import admin_principal, make_private


def _has_calibrations_for_annotation(*args, **kwargs) -> bool:
    """Whether any calibration survived both the eligibility and visibility checks.

    ``calibrations_available_for_annotation`` returns the surviving calibrations; the cases below assert
    only on whether the list was empty.
    """
    return bool(calibrations_available_for_annotation(*args, **kwargs))


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
class TestCalibrationsAvailableForAnnotation:
    """
    Unit tests for calibration availability, via the _has_calibrations_for_annotation adapter below.
    This function is used by both functional and pathogenicity annotation checks, so we test it separately here to avoid duplication in the tests for those checks.
    """

    @pytest.mark.parametrize("kind", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_calibrations_are_none(self, mock_annotation_context, kind):
        mock_annotation_context.variant.score_set.score_calibrations = None
        assert _has_calibrations_for_annotation(mock_annotation_context, kind) is False

    @pytest.mark.parametrize("kind", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_no_calibrations_present(self, mock_annotation_context, kind):
        mock_annotation_context.variant.score_set.score_calibrations = []
        assert _has_calibrations_for_annotation(mock_annotation_context, kind) is False

    @pytest.mark.parametrize("annotation_type", ["functional", "pathogenicity"], ids=["functional", "pathogenicity"])
    def test_score_range_check_returns_false_when_all_calibrations_are_research_use_only_and_not_allowed(
        self, mock_annotation_context_with_functional_calibration_score_set, annotation_type
    ):
        """Test that research use only calibrations are excluded by default."""
        for (
            calibration
        ) in mock_annotation_context_with_functional_calibration_score_set.variant.score_set.score_calibrations:
            calibration.research_use_only = True

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, annotation_type
            )
            is False
        )

    @pytest.mark.parametrize(
        "kind,context_fixture",
        [
            ("functional", "mock_annotation_context_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_annotation_context_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_true_when_research_use_only_calibrations_are_allowed(
        self, kind, context_fixture, request
    ):
        """Test that research use only calibrations are included when explicitly allowed."""
        context = request.getfixturevalue(context_fixture)
        for calibration in context.variant.score_set.score_calibrations:
            calibration.primary = False
            calibration.research_use_only = True

        assert _has_calibrations_for_annotation(context, kind, allow_research_use_only_calibrations=True) is True

    @pytest.mark.parametrize(
        "kind,context_fixture",
        [
            ("functional", "mock_annotation_context_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_annotation_context_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_false_when_calibrations_present_with_empty_ranges(
        self, kind, context_fixture, request
    ):
        context = request.getfixturevalue(context_fixture)

        for calibration in context.variant.score_set.score_calibrations:
            calibration.functional_classifications = None

        assert _has_calibrations_for_annotation(context, kind) is False

    def test_pathogenicity_range_check_returns_false_when_no_acmg_calibration(
        self,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        for (
            calibration
        ) in mock_annotation_context_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
            for fr in acmg_classification_removed:
                fr["acmgClassification"] = None

            calibration.functional_classifications = acmg_classification_removed

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is False
        )

    def test_pathogenicity_range_check_returns_true_when_some_acmg_calibration(
        self,
        mock_annotation_context_with_pathogenicity_calibration_score_set,
    ):
        for (
            calibration
        ) in mock_annotation_context_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations:
            acmg_classification_removed = [deepcopy(r) for r in calibration.functional_classifications]
            acmg_classification_removed[0]["acmgClassification"] = None

            calibration.functional_classifications = acmg_classification_removed

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is True
        )

    @pytest.mark.parametrize(
        "kind,context_fixture",
        [
            ("functional", "mock_annotation_context_with_functional_calibration_score_set"),
            ("pathogenicity", "mock_annotation_context_with_pathogenicity_calibration_score_set"),
        ],
        ids=["functional_fixture", "pathogenicity_fixture"],
    )
    def test_score_range_check_returns_true_when_calibration_kind_exists_with_ranges(
        self, kind, context_fixture, request
    ):
        context = request.getfixturevalue(context_fixture)

        assert _has_calibrations_for_annotation(context, kind) is True

    def test_score_range_check_returns_true_when_mixed_research_use_calibrations_exist_functional(
        self, mock_annotation_context_with_functional_calibration_score_set
    ):
        """Test behavior with mixed research use only and regular calibrations for functional annotation."""
        calibrations = (
            mock_annotation_context_with_functional_calibration_score_set.variant.score_set.score_calibrations
        )

        if len(calibrations) == 1:
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        calibrations[0].research_use_only = True
        calibrations[1].research_use_only = False

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional"
            )
            is True
        )

    def test_score_range_check_returns_true_when_mixed_research_use_calibrations_exist_pathogenicity(
        self, mock_annotation_context_with_pathogenicity_calibration_score_set
    ):
        """Test behavior with mixed research use only and regular calibrations for pathogenicity annotation."""
        calibrations = (
            mock_annotation_context_with_pathogenicity_calibration_score_set.variant.score_set.score_calibrations
        )

        if len(calibrations) == 1:
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        calibrations[0].research_use_only = True
        calibrations[1].research_use_only = False

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_pathogenicity_calibration_score_set, "pathogenicity"
            )
            is True
        )

    def test_score_range_check_handles_mixed_functional_classifications(
        self,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        """Test behavior when some calibrations have functional classifications and some don't."""
        calibrations = (
            mock_annotation_context_with_functional_calibration_score_set.variant.score_set.score_calibrations
        )

        if len(calibrations) == 1:
            new_calibration = deepcopy(calibrations[0])
            calibrations.append(new_calibration)

        calibrations[1].functional_classifications = None

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional"
            )
            is True
        )

    def test_pathogenicity_annotation_with_functional_classifications_but_no_acmg(
        self,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        """Test that pathogenicity annotation fails when functional classifications exist but have no ACMG classifications."""
        calibrations = (
            mock_annotation_context_with_functional_calibration_score_set.variant.score_set.score_calibrations
        )

        for calibration in calibrations:
            if hasattr(calibration, "functional_classifications") and calibration.functional_classifications:
                acmg_classification_removed = [deepcopy(fc) for fc in calibration.functional_classifications]
                for fc in acmg_classification_removed:
                    if "acmgClassification" in fc:
                        fc["acmgClassification"] = None
                calibration.functional_classifications = acmg_classification_removed

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "pathogenicity"
            )
            is False
        )

    def test_functional_annotation_with_empty_functional_classifications_list(
        self,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        """Test that functional annotation fails when functional classifications list is empty."""
        calibrations = (
            mock_annotation_context_with_functional_calibration_score_set.variant.score_set.score_calibrations
        )

        for calibration in calibrations:
            calibration.functional_classifications = []

        assert (
            _has_calibrations_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional"
            )
            is False
        )


@pytest.mark.unit
class TestCalibrationAvailabilityIsScopedToTheCaller:
    """A calibration's READ rule is stricter than its score set's, so publishing a score set does not
    publish its calibrations. These cases exist because this function once read
    ``score_set.score_calibrations`` directly and handed private calibrations to anyone.
    """

    def test_a_private_calibration_is_not_available_for_annotation(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert (
            calibrations_available_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional"
            )
            == []
        )

    def test_omitting_the_principal_withholds_rather_than_widens(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        # The whole design rests on an omitted principal meaning "the public". Passing an explicitly
        # anonymous principal and passing none at all must agree.
        make_private(mock_mapped_variant_with_functional_calibration_score_set)
        context = mock_annotation_context_with_functional_calibration_score_set

        assert calibrations_available_for_annotation(context, "functional") == calibrations_available_for_annotation(
            context, "functional", principal=Principal()
        )

    def test_an_entitled_caller_still_receives_a_private_calibration(
        self,
        mock_mapped_variant_with_functional_calibration_score_set,
        mock_annotation_context_with_functional_calibration_score_set,
    ):
        make_private(mock_mapped_variant_with_functional_calibration_score_set)

        assert (
            calibrations_available_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional", principal=admin_principal()
            )
            != []
        )

    def test_a_public_calibration_is_still_available_to_anyone(
        self, mock_annotation_context_with_functional_calibration_score_set
    ):
        # The counterweight: withholding private calibrations must not withhold what publishing released.
        assert (
            calibrations_available_for_annotation(
                mock_annotation_context_with_functional_calibration_score_set, "functional"
            )
            != []
        )


@pytest.mark.unit
class TestSelectStrongestFunctionalCalibrationUnit:
    """Unit tests for select_strongest_functional_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_annotation_context):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_functional_calibration(mock_annotation_context, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_annotation_context_with_functional_calibration_score_set):
        """Test that single calibration is returned."""
        context = mock_annotation_context_with_functional_calibration_score_set
        calibrations = context.variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_functional_calibration(context, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_returns_first_when_all_agree(self, mock_annotation_context_with_functional_calibration_score_set):
        """Test that first calibration is returned when all have same classification."""
        context = mock_annotation_context_with_functional_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
        calibration2 = deepcopy(calibration1)
        calibration2.id = 999
        calibrations = [calibration1, calibration2]

        calibration, functional_range = select_strongest_functional_calibration(context, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]  # Should return the first one

    def test_defaults_to_normal_on_conflict(self, mock_annotation_context_with_functional_calibration_score_set):
        """Test that normal classification is preferred when there are conflicts."""
        from unittest.mock import MagicMock, patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        context = mock_annotation_context_with_functional_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
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
            calibration, functional_range = select_strongest_functional_calibration(context, calibrations)

            # Should return the normal classification (second one)
            assert calibration == calibration2
            assert functional_range.label == "Normal"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_annotation_context_with_functional_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from mavedb.lib.annotation.classification import ExperimentalVariantFunctionalImpactClassification

        context = mock_annotation_context_with_functional_calibration_score_set
        calibrations = context.variant.score_set.score_calibrations

        # Mock to return None range but INDETERMINATE classification (variant not in any range)
        with patch(
            "mavedb.lib.annotation.calibration.functional_classification_of_variant",
            return_value=(None, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        ):
            calibration, functional_range = select_strongest_functional_calibration(context, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None


@pytest.mark.unit
class TestSelectStrongestPathogenicityCalibrationUnit:
    """Unit tests for select_strongest_pathogenicity_calibration function."""

    def test_returns_none_for_empty_calibrations(self, mock_annotation_context):
        """Test that empty calibration list returns None."""
        calibration, functional_range = select_strongest_pathogenicity_calibration(mock_annotation_context, [])
        assert calibration is None
        assert functional_range is None

    def test_returns_single_calibration(self, mock_annotation_context_with_pathogenicity_calibration_score_set):
        """Test that single calibration is returned."""
        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibrations = context.variant.score_set.score_calibrations

        calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

        assert calibration is not None
        assert calibration == calibrations[0]
        assert functional_range is not None

    def test_selects_strongest_evidence_strength(
        self, mock_annotation_context_with_pathogenicity_calibration_score_set
    ):
        """Test that calibration with strongest evidence is selected."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

            # Should return the one with VERY_STRONG evidence
            assert calibration == calibration2
            assert functional_range.label == "Strong"

    def test_defaults_to_uncertain_on_tie(self, mock_annotation_context_with_pathogenicity_calibration_score_set):
        """Test that uncertain significance is returned when benign and pathogenic evidence tie."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

            # Should return first calibration but None for range to indicate uncertain significance
            assert calibration == calibration1
            assert functional_range is None

    def test_returns_classification_when_all_tied_are_same_type(
        self, mock_annotation_context_with_pathogenicity_calibration_score_set
    ):
        """Test that classification is returned normally when all tied candidates are the same type."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

            # Should return first calibration with its range (no conflict, so normal classification)
            assert calibration == calibration1
            assert functional_range.label == "Benign1"

    def test_handles_none_evidence_strength(self, mock_annotation_context_with_pathogenicity_calibration_score_set):
        """Test that None evidence strength is handled correctly."""
        from unittest.mock import MagicMock, patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
        from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibration1 = context.variant.score_set.score_calibrations[0]
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
            calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

            # Should return the one with actual strength (second)
            assert calibration == calibration2
            assert functional_range.label == "Moderate"

    def test_returns_first_calibration_when_no_variants_in_ranges(
        self, mock_annotation_context_with_pathogenicity_calibration_score_set
    ):
        """Test that first calibration with None range is returned when variant is not in any functional range."""
        from unittest.mock import patch

        from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine

        context = mock_annotation_context_with_pathogenicity_calibration_score_set
        calibrations = context.variant.score_set.score_calibrations

        # Mock to return None range for all calibrations (variant not in any range)
        with patch(
            "mavedb.lib.annotation.calibration.pathogenicity_classification_of_variant",
            return_value=(None, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
        ):
            calibration, functional_range = select_strongest_pathogenicity_calibration(context, calibrations)

            # Should return first calibration with None range (indicating variant not in any range)
            assert calibration == calibrations[0]
            assert functional_range is None
