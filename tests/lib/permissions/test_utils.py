"""Tests for permissions utils module."""

import pytest

from mavedb.lib.permissions.utils import roles_permitted
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole


class TestRolesPermitted:
    """Test the roles_permitted utility function."""

    def test_user_role_permission_granted(self):
        """Test that permission is granted when user has a permitted role."""
        user_roles = [UserRole.admin, UserRole.mapper]
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is True

    def test_user_role_permission_denied(self):
        """Test that permission is denied when user lacks permitted roles."""
        user_roles = [UserRole.mapper]
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_contribution_role_permission_granted(self):
        """Test that permission is granted for contribution roles."""
        user_roles = [ContributionRole.admin, ContributionRole.editor]
        permitted_roles = [ContributionRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is True

    def test_contribution_role_permission_denied(self):
        """Test that permission is denied for contribution roles."""
        user_roles = [ContributionRole.viewer]
        permitted_roles = [ContributionRole.admin, ContributionRole.editor]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_empty_user_roles_permission_denied(self):
        """Test that permission is denied when user has no roles."""
        user_roles = []
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_multiple_matching_roles(self):
        """Test permission when user has multiple permitted roles."""
        user_roles = [UserRole.admin, UserRole.mapper]
        permitted_roles = [UserRole.admin, UserRole.mapper]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is True

    def test_partial_role_match(self):
        """Test permission when user has some but not all permitted roles."""
        user_roles = [UserRole.mapper]
        permitted_roles = [UserRole.admin, UserRole.mapper]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is True

    def test_no_role_overlap(self):
        """Test permission when user roles don't overlap with permitted roles."""
        user_roles = [ContributionRole.viewer]
        permitted_roles = [ContributionRole.admin, ContributionRole.editor]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_empty_permitted_roles(self):
        """Test behavior when no roles are permitted."""
        user_roles = [UserRole.admin]
        permitted_roles = []

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_both_empty_roles(self):
        """Test behavior when both user and permitted roles are empty."""
        user_roles = []
        permitted_roles = []

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_consistent_role_types_allowed(self):
        """Test behavior with consistent role types (should work fine)."""
        user_roles = [UserRole.admin]
        permitted_roles = [UserRole.admin, UserRole.mapper]
        assert roles_permitted(user_roles, permitted_roles) is True

        user_roles = [ContributionRole.editor]
        permitted_roles = [ContributionRole.admin, ContributionRole.editor, ContributionRole.viewer]
        assert roles_permitted(user_roles, permitted_roles) is True

    def test_mixed_user_role_types_raises_error(self):
        """Test that mixed role types in user_roles list raises ValueError."""
        permitted_roles = [UserRole.admin]
        mixed_user_roles = [UserRole.admin, ContributionRole.editor]

        with pytest.raises(ValueError) as exc_info:
            roles_permitted(mixed_user_roles, permitted_roles)

        assert "user_roles list cannot contain mixed role types" in str(exc_info.value)

    def test_mixed_permitted_role_types_raises_error(self):
        """Test that mixed role types in permitted_roles list raises ValueError."""
        user_roles = [UserRole.admin]
        mixed_permitted_roles = [UserRole.admin, ContributionRole.editor]

        with pytest.raises(ValueError) as exc_info:
            roles_permitted(user_roles, mixed_permitted_roles)

        assert "permitted_roles list cannot contain mixed role types" in str(exc_info.value)

    def test_different_role_types_between_lists_raises_error(self):
        """Test that different role types between lists raises ValueError."""
        user_roles = [UserRole.admin]
        permitted_roles = [ContributionRole.admin]

        with pytest.raises(ValueError) as exc_info:
            roles_permitted(user_roles, permitted_roles)

        assert "user_roles and permitted_roles must contain the same role type" in str(exc_info.value)

    def test_single_role_lists(self):
        """Test with single-item role lists."""
        user_roles = [UserRole.admin]
        permitted_roles = [UserRole.admin]
        assert roles_permitted(user_roles, permitted_roles) is True

        user_roles = [UserRole.mapper]
        permitted_roles = [UserRole.admin]
        assert roles_permitted(user_roles, permitted_roles) is False
