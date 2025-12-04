"""Tests for User permissions module."""

from typing import Callable, List
from unittest import mock

import pytest

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.user import (
    _handle_add_role_action,
    _handle_lookup_action,
    _handle_read_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

USER_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.LOOKUP: _handle_lookup_action,
    Action.ADD_ROLE: _handle_add_role_action,
}

USER_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.DELETE,
    Action.ADD_EXPERIMENT,
    Action.ADD_SCORE_SET,
    Action.ADD_BADGE,
    Action.CHANGE_RANK,
    Action.SET_SCORES,
    Action.PUBLISH,
]


def test_user_handles_all_actions() -> None:
    """Test that all User actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(USER_SUPPORTED_ACTIONS)
    unsupported = set(USER_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for users."


class TestUserHasPermission:
    """Test the main has_permission dispatcher function for User entities."""

    @pytest.mark.parametrize("action, handler", USER_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        user = entity_helper.create_user()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.user." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, user, action)
            mock_handler.assert_called_once_with(
                admin_user,
                user,
                False,  # admin is not viewing self
                [UserRole.admin],
            )

    @pytest.mark.parametrize("action", USER_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        user = entity_helper.create_user()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, user, action)

            error_msg = str(exc_info.value)
            assert action.value in error_msg
            assert all(a.value in error_msg for a in USER_SUPPORTED_ACTIONS)


class TestUserReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can read any User profile
            PermissionTest("User", None, "admin", Action.READ, True),
            # Users can read their own profile
            PermissionTest("User", None, "self", Action.READ, True),
            # Owners cannot read other user profiles (no special privilege)
            PermissionTest("User", None, "owner", Action.READ, False, 403),
            # Contributors cannot read other user profiles
            PermissionTest("User", None, "contributor", Action.READ, False, 403),
            # Mappers cannot read other user profiles
            PermissionTest("User", None, "mapper", Action.READ, False, 403),
            # Other users cannot read other user profiles
            PermissionTest("User", None, "other_user", Action.READ, False, 403),
            # Anonymous users cannot read user profiles
            PermissionTest("User", None, "anonymous", Action.READ, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        user = entity_helper.create_user()
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        user_is_self = test_case.user_type == "self"
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(user_data, user, user_is_self, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestUserUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can update any User profile
            PermissionTest("User", None, "admin", Action.UPDATE, True),
            # Users can update their own profile
            PermissionTest("User", None, "self", Action.UPDATE, True),
            # Owners cannot update other user profiles (no special privilege)
            PermissionTest("User", None, "owner", Action.UPDATE, False, 403),
            # Contributors cannot update other user profiles
            PermissionTest("User", None, "contributor", Action.UPDATE, False, 403),
            # Mappers cannot update other user profiles
            PermissionTest("User", None, "mapper", Action.UPDATE, False, 403),
            # Other users cannot update other user profiles
            PermissionTest("User", None, "other_user", Action.UPDATE, False, 403),
            # Anonymous users cannot update user profiles
            PermissionTest("User", None, "anonymous", Action.UPDATE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        user = entity_helper.create_user()
        user_data = entity_helper.create_user_data(test_case.user_type)

        user_is_self = test_case.user_type == "self"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(user_data, user, user_is_self, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestUserLookupActionHandler:
    """Test the _handle_lookup_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can lookup any User
            PermissionTest("User", None, "admin", Action.LOOKUP, True),
            # Users can lookup themselves
            PermissionTest("User", None, "self", Action.LOOKUP, True),
            # Owners can lookup other users (authenticated user privilege)
            PermissionTest("User", None, "owner", Action.LOOKUP, True),
            # Contributors can lookup other users (authenticated user privilege)
            PermissionTest("User", None, "contributor", Action.LOOKUP, True),
            # Mappers can lookup other users (authenticated user privilege)
            PermissionTest("User", None, "mapper", Action.LOOKUP, True),
            # Other authenticated users can lookup other users
            PermissionTest("User", None, "other_user", Action.LOOKUP, True),
            # Anonymous users cannot lookup users
            PermissionTest("User", None, "anonymous", Action.LOOKUP, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_lookup_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_lookup_action helper function directly."""
        user = entity_helper.create_user()
        user_data = entity_helper.create_user_data(test_case.user_type)

        user_is_self = test_case.user_type == "self"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_lookup_action(user_data, user, user_is_self, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestUserAddRoleActionHandler:
    """Test the _handle_add_role_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can add roles to any User
            PermissionTest("User", None, "admin", Action.ADD_ROLE, True),
            # Users cannot add roles to themselves
            PermissionTest("User", None, "self", Action.ADD_ROLE, False, 403),
            # Owners cannot add roles to other users
            PermissionTest("User", None, "owner", Action.ADD_ROLE, False, 403),
            # Contributors cannot add roles to other users
            PermissionTest("User", None, "contributor", Action.ADD_ROLE, False, 403),
            # Mappers cannot add roles to other users
            PermissionTest("User", None, "mapper", Action.ADD_ROLE, False, 403),
            # Other users cannot add roles to other users
            PermissionTest("User", None, "other_user", Action.ADD_ROLE, False, 403),
            # Anonymous users cannot add roles to users
            PermissionTest("User", None, "anonymous", Action.ADD_ROLE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_role_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_role_action helper function directly."""
        user = entity_helper.create_user()
        user_data = entity_helper.create_user_data(test_case.user_type)

        user_is_self = test_case.user_type == "self"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_role_action(user_data, user, user_is_self, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code
