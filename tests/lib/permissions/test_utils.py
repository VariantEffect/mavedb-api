# ruff: noqa: E402

"""Tests for permissions utils module."""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from unittest.mock import Mock

from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
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


class TestDenyActionForEntity:
    """Test the deny_action_for_entity utility function."""

    @pytest.mark.parametrize(
        "entity_is_private, user_data, user_can_view_private, expected_status",
        [
            # Private entity, anonymous user
            (True, None, False, 404),
            # Private entity, authenticated user without permissions
            (True, Mock(user=Mock(id=1)), False, 404),
            # Private entity, authenticated user with permissions
            (True, Mock(user=Mock(id=1)), True, 403),
            # Public entity, anonymous user
            (False, None, False, 401),
            # Public entity, authenticated user
            (False, Mock(user=Mock(id=1)), False, 403),
        ],
        ids=[
            "private_anonymous_not-viewer",
            "private_authenticated_not-viewer",
            "private_authenticated_viewer",
            "public_anonymous",
            "public_authenticated",
        ],
    )
    def test_deny_action(self, entity_is_private, user_data, user_can_view_private, expected_status):
        """Test denial for various user and entity privacy scenarios."""

        entity = Mock(urn="entity:1234")
        response = deny_action_for_entity(entity, entity_is_private, user_data, user_can_view_private)

        assert response.permitted is False
        assert response.http_code == expected_status

    def test_deny_action_urn_available(self):
        """Test denial message includes URN when available."""
        entity = Mock(urn="entity:5678")
        response = deny_action_for_entity(entity, True, None, False)

        assert "URN 'entity:5678'" in response.message

    def test_deny_action_id_available(self):
        """Test denial message includes ID when URN is not available."""
        entity = Mock(urn=None, id=42)
        response = deny_action_for_entity(entity, True, None, False)

        assert "ID '42'" in response.message

    def test_deny_action_no_identifier(self):
        """Test denial message when neither URN nor ID is available."""
        entity = Mock(urn=None, id=None)
        response = deny_action_for_entity(entity, True, None, False)

        assert "unknown" in response.message

    def test_deny_handles_undefined_attributres(self):
        """Test denial message when identifier attributes are undefined."""
        entity = Mock()
        del entity.urn  # Remove urn attribute
        del entity.id  # Remove id attribute
        response = deny_action_for_entity(entity, True, None, False)

        assert "unknown" in response.message

    def test_deny_action_entity_name_in_message(self):
        """Test denial message includes entity class name."""

        class CustomEntity:
            pass

        entity = CustomEntity()
        response = deny_action_for_entity(entity, True, None, False)

        assert "CustomEntity" in response.message
