from copy import deepcopy

import pytest

from mavedb.lib.exceptions import ValidationError
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided
from mavedb.view_models.acmg_classification import ACMGClassification, ACMGClassificationCreate
from tests.helpers.constants import (
    TEST_ACMG_BS3_STRONG_CLASSIFICATION,
    TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS,
    TEST_ACMG_PS3_STRONG_CLASSIFICATION,
    TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS,
    TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION,
    TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS,
    TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION,
    TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS,
)

### ACMG Classification Creation Tests ###


@pytest.mark.parametrize(
    "valid_acmg_classification",
    [
        TEST_ACMG_BS3_STRONG_CLASSIFICATION,
        TEST_ACMG_PS3_STRONG_CLASSIFICATION,
        TEST_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS,
        TEST_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS,
    ],
)
def test_can_create_acmg_classification(valid_acmg_classification):
    """Test that valid ACMG classifications can be created."""
    acmg = ACMGClassificationCreate(**valid_acmg_classification)

    assert isinstance(acmg, ACMGClassificationCreate)
    assert acmg.criterion.value == valid_acmg_classification.get("criterion")
    assert acmg.evidence_strength.value == valid_acmg_classification.get("evidence_strength")
    assert acmg.points == valid_acmg_classification.get("points")


def test_cannot_create_acmg_classification_with_mismatched_points():
    """Test that an ACMG classification cannot be created with mismatched points."""
    invalid_acmg_classification = deepcopy(TEST_ACMG_BS3_STRONG_CLASSIFICATION)
    invalid_acmg_classification["points"] = 2  # BS3 Strong should be -4 points

    with pytest.raises(ValidationError) as exc:
        ACMGClassificationCreate(**invalid_acmg_classification)

    assert "The provided points value does not agree with the provided criterion and evidence_strength" in str(
        exc.value
    )


def test_cannot_create_acmg_classification_with_only_criterion():
    """Test that an ACMG classification cannot be created with only criterion."""
    invalid_acmg_classification = deepcopy(TEST_ACMG_BS3_STRONG_CLASSIFICATION)
    invalid_acmg_classification.pop("evidence_strength")

    with pytest.raises(ValidationError) as exc:
        ACMGClassificationCreate(**invalid_acmg_classification)

    assert "Both a criterion and evidence_strength must be provided together" in str(exc.value)


def test_cannot_create_acmg_classification_with_only_evidence_strength():
    """Test that an ACMG classification cannot be created with only evidence_strength."""
    invalid_acmg_classification = deepcopy(TEST_ACMG_BS3_STRONG_CLASSIFICATION)
    invalid_acmg_classification.pop("criterion")

    with pytest.raises(ValidationError) as exc:
        ACMGClassificationCreate(**invalid_acmg_classification)

    assert "Both a criterion and evidence_strength must be provided together" in str(exc.value)


def test_can_create_acmg_classification_from_points():
    """Test that an ACMG classification can be created from points alone."""
    acmg = ACMGClassificationCreate(points=-4)  # BS3 Strong

    assert isinstance(acmg, ACMGClassificationCreate)
    assert acmg.criterion == ACMGCriterion.BS3
    assert acmg.evidence_strength == StrengthOfEvidenceProvided.STRONG
    assert acmg.points == -4


### ACMG Classification Saved Data Tests ###


@pytest.mark.parametrize(
    "valid_saved_classification",
    [
        TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION,
        TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION,
        TEST_SAVED_ACMG_BS3_STRONG_CLASSIFICATION_WITH_POINTS,
        TEST_SAVED_ACMG_PS3_STRONG_CLASSIFICATION_WITH_POINTS,
    ],
)
def test_can_create_acmg_classification_from_saved_data(valid_saved_classification):
    """Test that an ACMG classification can be created from saved data."""
    acmg = ACMGClassification(**valid_saved_classification)

    assert isinstance(acmg, ACMGClassification)
    assert acmg.criterion.value == valid_saved_classification.get("criterion")
    assert acmg.evidence_strength.value == valid_saved_classification.get("evidenceStrength")
    assert acmg.points == valid_saved_classification.get("points")
