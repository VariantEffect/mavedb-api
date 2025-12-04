"""Tests for permissions models module."""

from mavedb.lib.permissions.models import PermissionResponse


class TestPermissionResponse:
    """Test the PermissionResponse class."""

    def test_permitted_response_creation(self):
        """Test creating a PermissionResponse for permitted access."""
        response = PermissionResponse(permitted=True)

        assert response.permitted is True
        assert response.http_code is None
        assert response.message is None

    def test_denied_response_creation_with_defaults(self):
        """Test creating a PermissionResponse for denied access with default values."""
        response = PermissionResponse(permitted=False)

        assert response.permitted is False
        assert response.http_code == 403
        assert response.message is None

    def test_denied_response_creation_with_custom_values(self):
        """Test creating a PermissionResponse for denied access with custom values."""
        response = PermissionResponse(permitted=False, http_code=404, message="Resource not found")

        assert response.permitted is False
        assert response.http_code == 404
        assert response.message == "Resource not found"

    def test_permitted_response_ignores_error_parameters(self):
        """Test that permitted responses ignore http_code and message parameters."""
        response = PermissionResponse(permitted=True, http_code=404, message="This should be ignored")

        assert response.permitted is True
        assert response.http_code is None
        assert response.message is None
