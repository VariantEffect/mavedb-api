from mavedb.view_models.mapped_variant import MappedVariantCreate, MappedVariant

from tests.helpers.util.common import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_MINIMAL_MAPPED_VARIANT, TEST_MINIMAL_MAPPED_VARIANT_CREATE, VALID_VARIANT_URN


def test_minimal_mapped_variant_create():
    mapped_variant_create = MappedVariantCreate(**TEST_MINIMAL_MAPPED_VARIANT_CREATE)

    assert all(mapped_variant_create.__getattribute__(k) == v for k, v in TEST_MINIMAL_MAPPED_VARIANT_CREATE.items())


def test_minimal_mapped_variant():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1

    saved_mapped_variant = MappedVariant.model_validate(dummy_attributed_object_from_dict({**mapped_variant, "variant_urn": VALID_VARIANT_URN}))

    assert all(saved_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant.items())
