from mavedb.view_models.variant import VariantEffectMeasurementCreate, VariantEffectMeasurement

from tests.helpers.util.common import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_MINIMAL_VARIANT, TEST_POPULATED_VARIANT, TEST_SAVED_VARIANT


def test_minimal_variant_create():
    variant = VariantEffectMeasurementCreate(**TEST_MINIMAL_VARIANT, score_set_id=1)
    assert all(variant.__getattribute__(k) == v for k, v in TEST_MINIMAL_VARIANT.items())


def test_populated_variant_create():
    variant = VariantEffectMeasurementCreate(**TEST_POPULATED_VARIANT, score_set_id=1)
    assert all(variant.__getattribute__(k) == v for k, v in TEST_POPULATED_VARIANT.items())


def test_saved_variant():
    variant = VariantEffectMeasurement.model_validate(
        dummy_attributed_object_from_dict({**TEST_SAVED_VARIANT, "score_set_id": 1})
    )
    assert all(variant.__getattribute__(k) == v for k, v in TEST_SAVED_VARIANT.items())
