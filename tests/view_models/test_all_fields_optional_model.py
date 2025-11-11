from typing import Optional

import pytest
from pydantic import Field

from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.utils import all_fields_optional_model


# Test models
class DummyModel(BaseModel):
    required_string: str = Field(..., description="Required string field")
    required_int: int
    optional_with_default: str = "default_value"
    optional_nullable: Optional[str] = None
    field_with_constraints: int = Field(..., ge=0, le=100)
    optional_boolean: bool = True


def test_all_fields_optional_model_basic():
    """Test that all fields become optional in the decorated model."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    # Should be able to create instance with no arguments
    instance = OptionalDummyModel()

    assert instance.required_string is None
    assert instance.required_int is None
    assert instance.optional_with_default is None  # Default overridden to None
    assert instance.optional_nullable is None
    assert instance.field_with_constraints is None
    assert instance.optional_boolean is None


def test_all_fields_optional_model_partial_assignment():
    """Test that partial field assignment works correctly."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    instance = OptionalDummyModel(required_string="test", required_int=42)

    assert instance.required_string == "test"
    assert instance.required_int == 42
    assert instance.optional_with_default is None
    assert instance.optional_nullable is None
    assert instance.field_with_constraints is None
    assert instance.optional_boolean is None


def test_all_fields_optional_model_all_fields_provided():
    """Test that all fields can still be provided."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    instance = OptionalDummyModel(
        required_string="test",
        required_int=42,
        optional_with_default="custom_value",
        optional_nullable="not_null",
        field_with_constraints=50,
        optional_boolean=False,
    )

    assert instance.required_string == "test"
    assert instance.required_int == 42
    assert instance.optional_with_default == "custom_value"
    assert instance.optional_nullable == "not_null"
    assert instance.field_with_constraints == 50
    assert instance.optional_boolean is False


def test_all_fields_optional_model_field_info_preserved():
    """Test that field constraints and metadata are preserved."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    # Check that field info is preserved
    required_str_field = OptionalDummyModel.model_fields["required_string"]
    assert required_str_field.description == "Required string field"

    # Field should now be optional
    assert required_str_field.default is None


def test_all_fields_optional_model_validation_still_works():
    """Test that field validation still works when values are provided."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    # Should still validate constraints when value is provided
    with pytest.raises(ValueError):
        OptionalDummyModel(field_with_constraints=150)  # Exceeds max value of 100


def test_all_fields_optional_model_type_annotations():
    """Test that type annotations are correctly made optional."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    # Get field annotations
    fields = OptionalDummyModel.model_fields

    # Check that previously required fields are now Optional
    assert fields["required_string"].annotation == Optional[str]
    assert fields["required_int"].annotation == Optional[int]

    # Check that already optional fields remain optional
    assert fields["optional_nullable"].annotation == Optional[str]
    assert fields["optional_boolean"].annotation == Optional[bool]


def test_all_fields_optional_model_serialization():
    """Test that the optional model serializes correctly."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    instance = OptionalDummyModel(required_string="test")
    serialized = instance.model_dump()

    expected = {
        "required_string": "test",
        "required_int": None,
        "optional_with_default": None,
        "optional_nullable": None,
        "field_with_constraints": None,
        "optional_boolean": None,
    }

    assert serialized == expected


def test_all_fields_optional_model_exclude_unset():
    """Test that model_dump with exclude_unset works correctly."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    instance = OptionalDummyModel(required_string="test")
    serialized = instance.model_dump(exclude_unset=True)

    # Should only include explicitly set fields
    assert serialized == {"required_string": "test"}


def test_all_fields_optional_model_inheritance():
    """Test that inheritance still works with the decorated model."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    # Should inherit from DummyModel
    assert issubclass(OptionalDummyModel, DummyModel)
    assert issubclass(OptionalDummyModel, BaseModel)


def test_all_fields_optional_model_field_defaults_overridden():
    """Test that original defaults are overridden to None."""

    @all_fields_optional_model()
    class OptionalDummyModel(DummyModel):
        pass

    instance = OptionalDummyModel()

    # Originally had default True, should now be None
    assert instance.optional_boolean is None

    # Originally had default None, should still be None
    assert instance.optional_nullable is None
