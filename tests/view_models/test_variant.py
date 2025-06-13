from mavedb.view_models.variant import VariantCreate, Variant

from tests.helpers.util.common import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_MINIMAL_VARIANT, TEST_POPULATED_VARIANT, TEST_SAVED_VARIANT


def test_minimal_variant_create():
    variant = VariantCreate(**TEST_MINIMAL_VARIANT)
    assert all(variant.__getattribute__(k) == v for k, v in TEST_MINIMAL_VARIANT.items())


def test_populated_variant_create():
    variant = VariantCreate(**TEST_POPULATED_VARIANT)
    assert all(variant.__getattribute__(k) == v for k, v in TEST_POPULATED_VARIANT.items())


def test_saved_variant():
    variant = Variant.model_validate(dummy_attributed_object_from_dict(TEST_SAVED_VARIANT))
    assert all(variant.__getattribute__(k) == v for k, v in TEST_SAVED_VARIANT.items())
