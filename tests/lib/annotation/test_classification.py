import pytest

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

from mavedb.lib.annotation.classification import (
    functional_classification_of_variant,
    pillar_project_clinical_classification_of_variant,
    ExperimentalVariantFunctionalImpactClassification,
)


@pytest.mark.parametrize(
    "score,expected_classification",
    [
        (
            -4,
            ExperimentalVariantFunctionalImpactClassification.INDETERMINATE,
        ),
        (
            1,
            ExperimentalVariantFunctionalImpactClassification.NORMAL,
        ),
        (
            -1,
            ExperimentalVariantFunctionalImpactClassification.ABNORMAL,
        ),
    ],
)
def test_functional_classification_of_variant_with_ranges(mock_mapped_variant, score, expected_classification):
    mock_mapped_variant.variant.data["score_data"]["score"] = score

    result = functional_classification_of_variant(mock_mapped_variant)
    assert result == expected_classification


def test_functional_classification_of_variant_without_ranges(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_ranges = None

    with pytest.raises(ValueError) as exc:
        functional_classification_of_variant(mock_mapped_variant)

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score ranges" in str(exc.value)


@pytest.mark.parametrize(
    "score,expected_classification,expected_strength_of_evidence",
    [
        (0, VariantPathogenicityEvidenceLine.Criterion.PS3, None),
        (-1, VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.SUPPORTING),
        (1, VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.SUPPORTING),
        (-2, VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.MODERATE),
        (2, VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.MODERATE),
        (-4, VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
        (4, VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
        (-8, VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.VERY_STRONG),
        (8, VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
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
    mock_mapped_variant.variant.score_set.score_ranges = None

    with pytest.raises(ValueError) as exc:
        pillar_project_clinical_classification_of_variant(mock_mapped_variant)

    assert f"Variant {mock_mapped_variant.variant.urn} does not have a score set with score thresholds" in str(
        exc.value
    )
