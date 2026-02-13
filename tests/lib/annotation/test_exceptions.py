"""
Tests for mavedb.lib.annotation.exceptions module.

This module tests custom exception classes used in the annotation system.
"""

import pytest

from mavedb.lib.annotation.exceptions import (
    MappingDataDoesntExistException,
)


@pytest.mark.unit
class TestMappingDataDoesntExistException:
    """Unit tests for MappingDataDoesntExistException."""

    def test_exception_inheritance(self):
        """Test that MappingDataDoesntExistException inherits from ValueError."""
        assert issubclass(MappingDataDoesntExistException, ValueError)

    def test_exception_creation_with_message(self):
        """Test creating exception with a message."""
        message = "Test mapping data error"
        exception = MappingDataDoesntExistException(message)

        assert str(exception) == message
        assert isinstance(exception, ValueError)

    def test_exception_creation_without_message(self):
        """Test creating exception without a message."""
        exception = MappingDataDoesntExistException()

        assert str(exception) == ""
        assert isinstance(exception, ValueError)

    def test_exception_raising(self):
        """Test raising the exception."""
        with pytest.raises(MappingDataDoesntExistException):
            raise MappingDataDoesntExistException("Test error")

    def test_exception_catching_as_value_error(self):
        """Test that exception can be caught as ValueError."""
        with pytest.raises(ValueError):
            raise MappingDataDoesntExistException("Test error")

    def test_exception_with_multiple_args(self):
        """Test creating exception with multiple arguments."""
        exception = MappingDataDoesntExistException("Error", "Additional info")

        assert "Error" in str(exception)
        assert isinstance(exception, ValueError)
