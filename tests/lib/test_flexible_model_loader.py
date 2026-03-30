# ruff: noqa: E402

import pytest

pytest.importorskip("fastapi")

import json
from typing import Optional
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError

from mavedb.lib.flexible_model_loader import create_flexible_model_loader, json_or_form_loader
from mavedb.view_models.base.base import BaseModel


class SampleModel(BaseModel):
    """Sample model for flexible model loader tests."""

    name: str
    age: int
    email: Optional[str] = None


class ComplexSampleModel(BaseModel):
    """More complex sample model with validation."""

    id: int
    title: str
    tags: list[str] = []
    metadata: dict = {}


@pytest.fixture
def test_model_loader():
    """Create a flexible model loader for SampleModel."""
    return create_flexible_model_loader(SampleModel)


@pytest.fixture
def custom_loader():
    """Create a flexible model loader with custom parameters."""
    return create_flexible_model_loader(SampleModel, form_field_name="custom_field", error_detail_prefix="Custom error")


@pytest.fixture
def mock_request():
    """Create a mock FastAPI Request object."""
    request = Mock(spec=Request)
    request.body = AsyncMock()
    return request


class TestCreateFlexibleModelLoader:
    """Test suite for create_flexible_model_loader function."""

    @pytest.mark.asyncio
    async def test_load_from_form_field_valid_data(self, test_model_loader, mock_request):
        """Test loading valid data from form field."""
        test_data = {"name": "John", "age": 30, "email": "john@example.com"}
        json_data = json.dumps(test_data)

        result = await test_model_loader(mock_request, item=json_data)

        assert isinstance(result, SampleModel)
        assert result.name == "John"
        assert result.age == 30
        assert result.email == "john@example.com"

    @pytest.mark.asyncio
    async def test_load_from_form_field_minimal_data(self, test_model_loader, mock_request):
        """Test loading minimal valid data from form field."""
        test_data = {"name": "Jane", "age": 25}
        json_data = json.dumps(test_data)

        result = await test_model_loader(mock_request, item=json_data)

        assert isinstance(result, SampleModel)
        assert result.name == "Jane"
        assert result.age == 25
        assert result.email is None

    @pytest.mark.asyncio
    async def test_load_from_json_body_valid_data(self, test_model_loader, mock_request):
        """Test loading valid data from JSON body."""
        test_data = {"name": "Bob", "age": 35, "email": "bob@example.com"}
        json_bytes = json.dumps(test_data).encode("utf-8")
        mock_request.body.return_value = json_bytes

        result = await test_model_loader(mock_request, item=None)

        assert isinstance(result, SampleModel)
        assert result.name == "Bob"
        assert result.age == 35
        assert result.email == "bob@example.com"

    @pytest.mark.asyncio
    async def test_form_field_takes_priority_over_json_body(self, test_model_loader, mock_request):
        """Test that form field data takes priority over JSON body."""
        form_data = {"name": "FormUser", "age": 25}
        body_data = {"name": "BodyUser", "age": 30}

        json_form = json.dumps(form_data)
        json_body = json.dumps(body_data).encode("utf-8")
        mock_request.body.return_value = json_body

        result = await test_model_loader(mock_request, item=json_form)

        assert result.name == "FormUser"
        assert result.age == 25

    @pytest.mark.asyncio
    async def test_validation_error_from_form_field(self, test_model_loader, mock_request):
        """Test ValidationError handling for invalid form field data."""
        invalid_data = {"name": "John"}  # Missing required 'age' field
        json_data = json.dumps(invalid_data)

        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item=json_data)

        errors = exc_info.value.errors()
        assert len(errors) > 0
        assert any(error["loc"] == ("age",) for error in errors)

    @pytest.mark.asyncio
    async def test_validation_error_from_json_body(self, test_model_loader, mock_request):
        """Test ValidationError handling for invalid JSON body data."""
        invalid_data = {"age": 25}  # Missing required 'name' field
        json_bytes = json.dumps(invalid_data).encode("utf-8")
        mock_request.body.return_value = json_bytes

        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item=None)

        errors = exc_info.value.errors()
        assert len(errors) > 0
        assert any(error["loc"] == ("name",) for error in errors)

    @pytest.mark.asyncio
    async def test_invalid_json_syntax_form_field(self, test_model_loader, mock_request):
        """Test handling of invalid JSON syntax in form field."""
        invalid_json = '{"name": "John", "age":}'  # Invalid JSON

        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item=invalid_json)

        assert exc_info.value.errors()
        assert "json_invalid" in exc_info.value.errors()[0]["type"]

    @pytest.mark.asyncio
    async def test_invalid_json_syntax_body(self, test_model_loader, mock_request):
        """Test handling of invalid JSON syntax in request body."""
        invalid_json = b'{"name": "John", "age":}'  # Invalid JSON
        mock_request.body.return_value = invalid_json

        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item=None)

        assert exc_info.value.errors()
        assert "json_invalid" in exc_info.value.errors()[0]["type"]

    @pytest.mark.asyncio
    async def test_empty_request_body_and_no_form_field(self, test_model_loader, mock_request):
        """Test handling when no data is provided in either form field or body."""
        mock_request.body.return_value = b""

        with pytest.raises(HTTPException) as exc_info:
            await test_model_loader(mock_request, item=None)

        assert exc_info.value.status_code == 422
        assert "No data provided in form field or request body" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_custom_error_detail_prefix(self, custom_loader, mock_request):
        """Test custom error detail prefix is used in error messages."""
        mock_request.body.return_value = b""

        with pytest.raises(HTTPException) as exc_info:
            await custom_loader(mock_request, item=None)

        assert exc_info.value.status_code == 422
        assert "Custom error" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_complex_model_with_nested_data(self, mock_request):
        """Test loading complex model with nested data structures."""
        complex_loader = create_flexible_model_loader(ComplexSampleModel)
        test_data = {
            "id": 1,
            "title": "Test Item",
            "tags": ["tag1", "tag2", "tag3"],
            "metadata": {"key1": "value1", "key2": {"nested": "value"}},
        }
        json_data = json.dumps(test_data)

        result = await complex_loader(mock_request, item=json_data)

        assert isinstance(result, ComplexSampleModel)
        assert result.id == 1
        assert result.title == "Test Item"
        assert result.tags == ["tag1", "tag2", "tag3"]
        assert result.metadata == {"key1": "value1", "key2": {"nested": "value"}}

    @pytest.mark.asyncio
    async def test_form_field_name_parameter_documentation_only(self, mock_request):
        """Test that form_field_name parameter doesn't affect functionality."""
        # Create loaders with different form_field_name values
        loader1 = create_flexible_model_loader(SampleModel, form_field_name="item")
        loader2 = create_flexible_model_loader(SampleModel, form_field_name="custom_name")

        test_data = {"name": "Test", "age": 30}
        json_data = json.dumps(test_data)

        # Both should work the same way since form_field_name is for docs only
        result1 = await loader1(mock_request, item=json_data)
        result2 = await loader2(mock_request, item=json_data)

        assert result1.name == result2.name == "Test"
        assert result1.age == result2.age == 30

    @pytest.mark.asyncio
    async def test_exception_handling_for_unexpected_errors(self, test_model_loader, mock_request):
        """Test handling of unexpected exceptions during processing."""
        # Mock an exception during model validation
        with patch.object(SampleModel, "model_validate_json", side_effect=RuntimeError("Unexpected error")):
            test_data = {"name": "John", "age": 30}
            json_data = json.dumps(test_data)

            with pytest.raises(HTTPException) as exc_info:
                await test_model_loader(mock_request, item=json_data)

            assert exc_info.value.status_code == 422
            assert "Unexpected error" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_unicode_data_handling(self, test_model_loader, mock_request):
        """Test handling of unicode characters in data."""
        test_data = {"name": "José María", "age": 25, "email": "josé@example.com"}
        json_data = json.dumps(test_data, ensure_ascii=False)

        result = await test_model_loader(mock_request, item=json_data)

        assert result.name == "José María"
        assert result.email == "josé@example.com"


class TestJsonOrFormLoader:
    """Test suite for json_or_form_loader convenience function."""

    @pytest.mark.asyncio
    async def test_convenience_function_basic_usage(self, mock_request):
        """Test the convenience function with basic usage."""
        loader = json_or_form_loader(SampleModel)
        test_data = {"name": "Alice", "age": 28}
        json_data = json.dumps(test_data)

        result = await loader(mock_request, item=json_data)

        assert isinstance(result, SampleModel)
        assert result.name == "Alice"
        assert result.age == 28

    @pytest.mark.asyncio
    async def test_convenience_function_custom_field_name(self, mock_request):
        """Test the convenience function with custom field name."""
        loader = json_or_form_loader(SampleModel, field_name="custom_field")
        test_data = {"name": "Charlie", "age": 35}
        json_data = json.dumps(test_data)

        result = await loader(mock_request, item=json_data)

        assert isinstance(result, SampleModel)
        assert result.name == "Charlie"
        assert result.age == 35

    @pytest.mark.asyncio
    async def test_convenience_function_error_message_format(self, mock_request):
        """Test that convenience function generates appropriate error messages."""
        loader = json_or_form_loader(SampleModel)
        mock_request.body.return_value = b""

        with pytest.raises(HTTPException) as exc_info:
            await loader(mock_request, item=None)

        assert exc_info.value.status_code == 422
        assert "Invalid SampleModel data" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_convenience_function_with_complex_model(self, mock_request):
        """Test convenience function with more complex model."""
        loader = json_or_form_loader(ComplexSampleModel)
        test_data = {"id": 42, "title": "Complex Test", "tags": ["test", "complex"], "metadata": {"source": "test"}}
        json_data = json.dumps(test_data)

        result = await loader(mock_request, item=json_data)

        assert isinstance(result, ComplexSampleModel)
        assert result.id == 42
        assert result.title == "Complex Test"
        assert result.tags == ["test", "complex"]
        assert result.metadata == {"source": "test"}


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_string_form_field(self, test_model_loader, mock_request):
        """Test handling of empty string in form field."""
        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item="")

        assert exc_info.value.errors()
        assert "json_invalid" in exc_info.value.errors()[0]["type"]

    @pytest.mark.asyncio
    async def test_whitespace_only_form_field(self, test_model_loader, mock_request):
        """Test handling of whitespace-only form field."""
        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item="   ")

        assert exc_info.value.errors()
        assert "json_invalid" in exc_info.value.errors()[0]["type"]

    @pytest.mark.asyncio
    async def test_null_json_value(self, test_model_loader, mock_request):
        """Test handling of null JSON value."""
        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item="null")

        assert exc_info.value.errors()
        assert "model_type" in exc_info.value.errors()[0]["type"]

    @pytest.mark.asyncio
    async def test_array_json_value(self, test_model_loader, mock_request):
        """Test handling of array JSON value instead of object."""
        with pytest.raises(RequestValidationError) as exc_info:
            await test_model_loader(mock_request, item='["not", "an", "object"]')

        assert exc_info.value.errors()
        assert "model_type" in exc_info.value.errors()[0]["type"]
