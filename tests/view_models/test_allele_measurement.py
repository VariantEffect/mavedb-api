# ruff: noqa: E402

import pytest
from pydantic import ValidationError

fastapi = pytest.importorskip("fastapi")

from mavedb.view_models.allele_measurement import AlleleMeasurement
from tests.helpers.constants import TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL, TEST_SAVED_FUNCTIONAL_RANGE_NORMAL

MEASUREMENT = {
    "variantUrn": "urn:mavedb:00000001-a-1#1",
    "relationship": "direct",
    "scoreSetUrn": "urn:mavedb:00000001-a-1",
    "scoreSetTitle": "Score set",
    "isCurrent": True,
}


@pytest.mark.parametrize(
    "classifications",
    [
        {},
        {"preferredClassification": TEST_SAVED_FUNCTIONAL_RANGE_NORMAL},
        {"researchUseOnlyClassification": TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL},
    ],
)
def test_allele_measurement_accepts_at_most_one_classification(classifications):
    measurement = AlleleMeasurement.model_validate({**MEASUREMENT, **classifications})

    assert (measurement.preferred_classification is None) or (measurement.research_use_only_classification is None)


def test_allele_measurement_rejects_both_classifications():
    with pytest.raises(ValidationError, match="researchUseOnlyClassification may only be set"):
        AlleleMeasurement.model_validate(
            {
                **MEASUREMENT,
                "preferredClassification": TEST_SAVED_FUNCTIONAL_RANGE_NORMAL,
                "researchUseOnlyClassification": TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL,
            }
        )
