import pytest

from mavedb.lib.annotation.classification import (
    functional_classification_of_variant,
    pillar_project_clinical_classification_of_variant,
    ExperimentalVariantFunctionalImpactClassification,
)
from ga4gh.va_spec.acmg_2015 import EvidenceOutcome
from ga4gh.va_spec.base import StrengthOfEvidenceProvided


@pytest.mark.parametrize(
    "score,expected_classification",
    [
        (0, ExperimentalVariantFunctionalImpactClassification.INDETERMINATE),
        (1, ExperimentalVariantFunctionalImpactClassification.NORMAL),
        (-1, ExperimentalVariantFunctionalImpactClassification.ABNORMAL),
    ],
)
def test_functional_classification_of_variant_with_ranges(mock_mapped_variant, score, expected_classification):
    mock_mapped_variant.variant.data["score_data"]["score"] = score

    result = functional_classification_of_variant(mock_mapped_variant)
    assert result == expected_classification


def test_functional_classification_of_variant_without_ranges(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None

    result = functional_classification_of_variant(mock_mapped_variant)
    assert result is None


@pytest.mark.parametrize(
    "score,expected_classification,expected_strength_of_evidence",
    [
        (0, None, None),
        (-1, EvidenceOutcome.BS3_SUPPORTING, StrengthOfEvidenceProvided.SUPPORTING),
        (1, EvidenceOutcome.PS3_SUPPORTING, StrengthOfEvidenceProvided.SUPPORTING),
        (-2, EvidenceOutcome.BS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
        (2, EvidenceOutcome.PS3_MODERATE, StrengthOfEvidenceProvided.MODERATE),
        (-4, EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
        (4, EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
        (-8, EvidenceOutcome.BS3, StrengthOfEvidenceProvided.STRONG),
        (8, EvidenceOutcome.PS3, StrengthOfEvidenceProvided.STRONG),
    ],
)
def test_clinical_classification_of_variant_with_thresholds(
    score, mock_mapped_variant, expected_classification, expected_strength_of_evidence
):
    mock_mapped_variant.variant.data["score_data"]["score"] = score

    classification, strength = pillar_project_clinical_classification_of_variant(mock_mapped_variant)
    assert classification == expected_classification
    assert strength == expected_strength_of_evidence


def test_clinical_classification_of_variant_without_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_calibrations = None

    classification, strength = pillar_project_clinical_classification_of_variant(mock_mapped_variant)
    assert classification is None
    assert strength is None
