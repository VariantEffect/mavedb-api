import pytest

from mavedb.lib.annotation.flatten import FlatAnnotation, flatten_annotation
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided
from tests.helpers.mocks.factories import (
    create_mock_acmg_classification,
    create_mock_functional_classification,
    create_mock_mapped_variant,
    create_mock_score_calibration,
    create_mock_score_set,
)


def _calibration_and_variant(functional_classifications, **calibration_kwargs):
    """Build a mock mapped variant whose score set carries a single calibration with *functional_classifications*."""
    score_set = create_mock_score_set()
    calibration = create_mock_score_calibration(
        functional_classifications=functional_classifications,
        score_set=score_set,
        **calibration_kwargs,
    )
    score_set.score_calibrations = [calibration]
    return calibration, create_mock_mapped_variant(score_set=score_set)


def _pathogenicity_calibration_and_variant(
    criterion=ACMGCriterion.PS3,
    evidence_strength=StrengthOfEvidenceProvided.STRONG,
    variant_in_range=True,
    **calibration_kwargs,
):
    classification = create_mock_functional_classification(
        functional_classification=FunctionalClassificationOptions.abnormal,
        label="Abnormal Range",
        range_values=[0.7, 1.0],
        acmg_classification=create_mock_acmg_classification(
            criterion=criterion,
            evidence_strength=evidence_strength,
        ),
        variant_in_range=variant_in_range,
    )
    return _calibration_and_variant([classification], **calibration_kwargs)


class TestFlattenAnnotation:
    """Tests that flatten_annotation projects a calibration's interpretation onto scalar fields."""

    def test_pathogenicity_calibration_populates_all_fields(self):
        calibration, mapped_variant = _pathogenicity_calibration_and_variant(
            urn="urn:mavedb:calibration:1", title="Clinical Calibration"
        )

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.functional_classification == "abnormal"
        assert annotation.acmg_criterion == "PS3"
        assert annotation.acmg_evidence_strength == "STRONG"
        assert annotation.acmg_evidence_outcome_code == "PS3"
        assert annotation.pathogenicity_classification == "PATHOGENIC"
        assert annotation.calibration_urn == "urn:mavedb:calibration:1"
        assert annotation.calibration_title == "Clinical Calibration"

    def test_benign_criterion_yields_benign_classification(self):
        calibration, mapped_variant = _pathogenicity_calibration_and_variant(criterion=ACMGCriterion.BS3)

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.acmg_criterion == "BS3"
        assert annotation.pathogenicity_classification == "BENIGN"

    def test_functional_only_calibration_omits_acmg_fields(self):
        classification = create_mock_functional_classification(
            functional_classification=FunctionalClassificationOptions.normal,
            label="Normal Range",
            range_values=[-1.0, 0.3],
        )
        calibration, mapped_variant = _calibration_and_variant(
            [classification], urn="urn:mavedb:calibration:2", title="Functional Calibration"
        )

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.functional_classification == "normal"
        assert annotation.acmg_criterion is None
        assert annotation.acmg_evidence_strength is None
        assert annotation.acmg_evidence_outcome_code is None
        assert annotation.pathogenicity_classification is None
        assert annotation.calibration_urn == "urn:mavedb:calibration:2"
        assert annotation.calibration_title == "Functional Calibration"

    def test_no_calibration_yields_empty_annotation(self):
        mapped_variant = create_mock_mapped_variant()

        assert flatten_annotation(mapped_variant, None) == FlatAnnotation()

    def test_calibration_without_ranges_keeps_its_identity(self):
        """Which calibration was consulted is known even when it can classify nothing.

        Reporting it is what distinguishes a calibration that defines no ranges from no calibration at
        all, and the public dump carries the former.
        """
        calibration, mapped_variant = _calibration_and_variant(
            [], urn="urn:mavedb:calibration:3", title="Baseline Only"
        )

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.calibration_urn == "urn:mavedb:calibration:3"
        assert annotation.calibration_title == "Baseline Only"
        assert annotation.research_use_only is False
        assert annotation.functional_classification is None
        assert annotation.acmg_criterion is None
        assert annotation.acmg_evidence_strength is None
        assert annotation.acmg_evidence_outcome_code is None
        assert annotation.pathogenicity_classification is None

    def test_variant_outside_all_ranges_is_uncertain(self):
        calibration, mapped_variant = _pathogenicity_calibration_and_variant(variant_in_range=False)

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.functional_classification == "indeterminate"
        assert annotation.acmg_criterion == "PS3"
        assert annotation.acmg_evidence_strength is None
        assert annotation.acmg_evidence_outcome_code == "PS3_not_met"
        assert annotation.pathogenicity_classification == "UNCERTAIN_SIGNIFICANCE"

    @pytest.mark.parametrize(
        "criterion,evidence_strength,expected_code",
        [
            (ACMGCriterion.PS3, StrengthOfEvidenceProvided.STRONG, "PS3"),
            (ACMGCriterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG, "PS3_very_strong"),
            (ACMGCriterion.PS3, StrengthOfEvidenceProvided.MODERATE, "PS3_moderate"),
            (ACMGCriterion.PS3, StrengthOfEvidenceProvided.SUPPORTING, "PS3_supporting"),
            (ACMGCriterion.BS3, StrengthOfEvidenceProvided.STRONG, "BS3"),
            (ACMGCriterion.BS3, StrengthOfEvidenceProvided.SUPPORTING, "BS3_supporting"),
        ],
    )
    def test_evidence_outcome_code(self, criterion, evidence_strength, expected_code):
        calibration, mapped_variant = _pathogenicity_calibration_and_variant(
            criterion=criterion, evidence_strength=evidence_strength
        )

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.acmg_evidence_outcome_code == expected_code

    def test_moderate_plus_is_preserved(self):
        """The VA-Spec annotations must collapse M+ to moderate; a CSV has no such obligation."""
        calibration, mapped_variant = _pathogenicity_calibration_and_variant(
            evidence_strength=StrengthOfEvidenceProvided.MODERATE_PLUS
        )

        annotation = flatten_annotation(mapped_variant, calibration)

        assert annotation.acmg_evidence_strength == "MODERATE_PLUS"
        assert annotation.acmg_evidence_outcome_code == "PS3_moderate_plus"
        assert annotation.pathogenicity_classification == "PATHOGENIC"
