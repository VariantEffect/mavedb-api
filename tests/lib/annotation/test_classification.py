import pytest

from mavedb.lib.annotation.classification import functional_classification_of_variant
from ga4gh.va_spec.profiles.assay_var_effect import AveFunctionalClassification, AveClinicalClassification
from mavedb.lib.annotation.classification import pillar_project_clinical_classification_of_variant


@pytest.mark.parametrize(
    "score,expected_classification",
    [
        (0, AveFunctionalClassification.INDETERMINATE),
        (1, AveFunctionalClassification.NORMAL),
        (-1, AveFunctionalClassification.ABNORMAL),
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
    "score,expected_classification",
    [
        (0, None),
        (-1, AveClinicalClassification.BS3_SUPPORTING),
        (1, AveClinicalClassification.PS3_SUPPORTING),
        (-2, AveClinicalClassification.BS3_MODERATE),
        (2, AveClinicalClassification.PS3_MODERATE),
        (-4, AveClinicalClassification.BS3_STRONG),
        (4, AveClinicalClassification.PS3_STRONG),
        (-8, AveClinicalClassification.BS3_STRONG),
        (8, AveClinicalClassification.PS3_STRONG),
    ],
)
def test_clinical_classification_of_variant_with_thresholds(score, mock_mapped_variant, expected_classification):
    mock_mapped_variant.variant.data["score_data"]["score"] = score

    result = pillar_project_clinical_classification_of_variant(mock_mapped_variant)
    assert result == expected_classification


def test_clinical_classification_of_variant_without_thresholds(mock_mapped_variant):
    mock_mapped_variant.variant.score_set.score_calibrations = None

    result = pillar_project_clinical_classification_of_variant(mock_mapped_variant)
    assert result is None
