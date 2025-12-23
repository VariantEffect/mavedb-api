import pytest
from pydantic import ValidationError

from mavedb.view_models.mapped_variant import MappedVariant, MappedVariantCreate
from tests.helpers.constants import TEST_MINIMAL_MAPPED_VARIANT, TEST_MINIMAL_MAPPED_VARIANT_CREATE, VALID_VARIANT_URN
from tests.helpers.util.common import dummy_attributed_object_from_dict


def test_minimal_mapped_variant_create():
    mapped_variant_create = TEST_MINIMAL_MAPPED_VARIANT_CREATE.copy()
    mapped_variant_create["variant_urn"] = VALID_VARIANT_URN
    created_mapped_variant = MappedVariantCreate(**mapped_variant_create)

    assert all(created_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant_create.items())


def test_can_use_variant_object_for_variant_urn_in_mapped_variant_create():
    mapped_variant_create = TEST_MINIMAL_MAPPED_VARIANT_CREATE.copy()
    created_mapped_variant = MappedVariantCreate(**{**mapped_variant_create, "variant": {"urn": VALID_VARIANT_URN}})

    assert all(created_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant_create.items())
    assert created_mapped_variant.variant_urn == VALID_VARIANT_URN


def test_minimal_mapped_variant():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1
    mapped_variant["variant_urn"] = VALID_VARIANT_URN

    saved_mapped_variant = MappedVariant.model_validate(dummy_attributed_object_from_dict({**mapped_variant}))

    assert all(saved_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant.items())


def test_can_use_variant_object_for_variant_urn_in_mapped_variant():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1

    saved_mapped_variant = MappedVariant.model_validate(
        dummy_attributed_object_from_dict(
            {**mapped_variant, "variant": dummy_attributed_object_from_dict({"urn": VALID_VARIANT_URN})}
        )
    )

    assert all(saved_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant.items())
    assert saved_mapped_variant.variant_urn == VALID_VARIANT_URN


def test_cannot_create_mapped_variant_without_variant_urn():
    mapped_variant_create = TEST_MINIMAL_MAPPED_VARIANT_CREATE.copy()
    mapped_variant_create["variant_urn"] = None

    with pytest.raises(ValidationError):
        MappedVariantCreate(**mapped_variant_create)


def test_cannot_save_mapped_variant_without_variant_urn():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1
    mapped_variant["variant_urn"] = None

    with pytest.raises(ValidationError):
        MappedVariant.model_validate(dummy_attributed_object_from_dict({**mapped_variant}))


def test_cannot_create_mapped_variant_without_variant():
    mapped_variant_create = TEST_MINIMAL_MAPPED_VARIANT_CREATE.copy()
    mapped_variant_create["variant"] = {"urn": None}

    with pytest.raises(ValidationError):
        MappedVariantCreate(**mapped_variant_create)


def test_can_create_saved_mapped_variant_with_variant_object():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1

    saved_mapped_variant = MappedVariant.model_validate(
        dummy_attributed_object_from_dict(
            {**mapped_variant, "variant": dummy_attributed_object_from_dict({"urn": VALID_VARIANT_URN})}
        )
    )

    assert all(saved_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant.items())
    assert saved_mapped_variant.variant_urn == VALID_VARIANT_URN


def test_cannot_save_mapped_variant_without_variant():
    mapped_variant = TEST_MINIMAL_MAPPED_VARIANT.copy()
    mapped_variant["id"] = 1
    mapped_variant["variant"] = None

    with pytest.raises(ValidationError):
        MappedVariant.model_validate(dummy_attributed_object_from_dict({**mapped_variant}))


def test_can_create_mapped_variant_from_non_orm_context():
    mapped_variant_create = TEST_MINIMAL_MAPPED_VARIANT_CREATE.copy()
    mapped_variant_create["variant_urn"] = VALID_VARIANT_URN
    created_mapped_variant = MappedVariantCreate.model_validate(mapped_variant_create)

    assert all(created_mapped_variant.__getattribute__(k) == v for k, v in mapped_variant_create.items())
