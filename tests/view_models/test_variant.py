from mavedb.view_models.variant import (
    SavedVariantEffectMeasurementWithMappedVariant,
    VariantEffectMeasurement,
    VariantEffectMeasurementCreate,
)
from tests.helpers.constants import (
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_MINIMAL_VARIANT,
    TEST_POPULATED_VARIANT,
    TEST_SAVED_VARIANT,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict


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


def test_can_create_saved_variant_with_mapping_with_all_attributed_properties():
    variant = TEST_SAVED_VARIANT.copy()
    variant["score_set_id"] = 1
    variant["mapped_variants"] = [
        dummy_attributed_object_from_dict(
            {
                **TEST_MINIMAL_MAPPED_VARIANT,
                "id": 1,
                "variant": dummy_attributed_object_from_dict({"urn": "urn:mavedb:variant-xxx"}),
            }
        )
    ]
    variant_attributed_object = dummy_attributed_object_from_dict(variant)
    saved_variant = SavedVariantEffectMeasurementWithMappedVariant.model_validate(variant_attributed_object)
    assert saved_variant.mapped_variant is not None
    assert saved_variant.mapped_variant.variant_urn == "urn:mavedb:variant-xxx"
    for k, v in TEST_SAVED_VARIANT.items():
        assert saved_variant.__getattribute__(k) == v


# Missing attributed properties here are unproblematic, as they are optional on the view model.
def test_can_create_saved_variant_with_mapping_with_missing_attributed_properties():
    variant = TEST_SAVED_VARIANT.copy()
    variant.pop("mapped_variants", None)
    variant["score_set_id"] = 1

    variant_attributed_object = dummy_attributed_object_from_dict(variant)
    saved_variant = SavedVariantEffectMeasurementWithMappedVariant.model_validate(variant_attributed_object)
    for k, v in TEST_SAVED_VARIANT.items():
        assert saved_variant.__getattribute__(k) == v


def test_can_create_saved_variant_with_mapping_from_non_orm_context():
    variant = TEST_SAVED_VARIANT.copy()
    variant["score_set_id"] = 1
    variant["mapped_variant"] = {
        **TEST_MINIMAL_MAPPED_VARIANT,
        "id": 1,
        "variant_urn": "urn:mavedb:variant-xxx",
    }

    saved_variant = SavedVariantEffectMeasurementWithMappedVariant.model_validate(variant)
    assert saved_variant.mapped_variant is not None
    assert saved_variant.mapped_variant.variant_urn == "urn:mavedb:variant-xxx"
    for k, v in TEST_SAVED_VARIANT.items():
        assert saved_variant.__getattribute__(k) == v
