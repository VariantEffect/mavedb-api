from mavedb.view_models.allele_measurement import AlleleMeasurement
from tests.helpers.constants import TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL

MEASUREMENT = {
    "variantUrn": "urn:mavedb:00000001-a-1#1",
    "relationship": "direct",
    "scoreSetUrn": "urn:mavedb:00000001-a-1",
    "scoreSetTitle": "Score set",
    "isCurrent": True,
}


def test_allele_measurement_classification_defaults_to_clinical():
    measurement = AlleleMeasurement.model_validate(
        {**MEASUREMENT, "classification": TEST_SAVED_FUNCTIONAL_RANGE_ABNORMAL}
    )

    assert measurement.classification is not None
    assert not measurement.classification_is_research_use_only
