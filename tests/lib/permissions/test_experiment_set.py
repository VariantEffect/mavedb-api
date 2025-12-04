"""Tests for ExperimentSet permissions module."""

from typing import Callable, List
from unittest import mock

import pytest

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.experiment_set import (
    _deny_action_for_experiment_set,
    _handle_add_experiment_action,
    _handle_delete_action,
    _handle_read_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

EXPERIMENT_SET_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.DELETE: _handle_delete_action,
    Action.ADD_EXPERIMENT: _handle_add_experiment_action,
}

EXPERIMENT_SET_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.ADD_SCORE_SET,
    Action.ADD_ROLE,
    Action.LOOKUP,
    Action.ADD_BADGE,
    Action.CHANGE_RANK,
    Action.SET_SCORES,
    Action.PUBLISH,
]


def test_experiment_set_handles_all_actions() -> None:
    """Test that all ExperimentSet actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(EXPERIMENT_SET_SUPPORTED_ACTIONS)
    unsupported = set(EXPERIMENT_SET_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for experiment sets."


class TestExperimentSetHasPermission:
    """Test the main has_permission dispatcher function for ExperimentSet entities."""

    @pytest.mark.parametrize("action, handler", EXPERIMENT_SET_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        experiment_set = entity_helper.create_experiment_set()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.experiment_set." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, experiment_set, action)
            mock_handler.assert_called_once_with(
                admin_user,
                experiment_set,
                experiment_set.private,
                False,  # admin is not the owner
                False,  # admin is not a contributor
                [UserRole.admin],
            )

    @pytest.mark.parametrize("action", EXPERIMENT_SET_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        experiment_set = entity_helper.create_experiment_set()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, experiment_set, action)

        error_msg = str(exc_info.value)
        assert action.value in error_msg
        assert all(a.value in error_msg for a in EXPERIMENT_SET_SUPPORTED_ACTIONS)

    def test_requires_private_attribute(self, entity_helper: EntityTestHelper) -> None:
        """Test that ValueError is raised if ExperimentSet.private is None."""
        experiment_set = entity_helper.create_experiment_set()
        experiment_set.private = None
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(ValueError) as exc_info:
            has_permission(admin_user, experiment_set, Action.READ)

        assert "private" in str(exc_info.value)


class TestExperimentSetReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can read any ExperimentSet
            PermissionTest("ExperimentSet", "published", "admin", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "admin", Action.READ, True),
            # Owners can read any ExperimentSet they own
            PermissionTest("ExperimentSet", "published", "owner", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "owner", Action.READ, True),
            # Contributors can read any ExperimentSet they contribute to
            PermissionTest("ExperimentSet", "published", "contributor", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "contributor", Action.READ, True),
            # Mappers can read any ExperimentSet (including private)
            PermissionTest("ExperimentSet", "published", "mapper", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "mapper", Action.READ, True),
            # Other users can only read published ExperimentSets
            PermissionTest("ExperimentSet", "published", "other_user", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "other_user", Action.READ, False, 404),
            # Anonymous users can only read published ExperimentSets
            PermissionTest("ExperimentSet", "published", "anonymous", Action.READ, True),
            PermissionTest("ExperimentSet", "private", "anonymous", Action.READ, False, 404),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        assert test_case.entity_state is not None, "ExperimentSet tests must have entity_state"
        experiment_set = entity_helper.create_experiment_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(
            user_data, experiment_set, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentSetUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can update any ExperimentSet
            PermissionTest("ExperimentSet", "private", "admin", Action.UPDATE, True),
            PermissionTest("ExperimentSet", "published", "admin", Action.UPDATE, True),
            # Owners can update any ExperimentSet they own
            PermissionTest("ExperimentSet", "private", "owner", Action.UPDATE, True),
            PermissionTest("ExperimentSet", "published", "owner", Action.UPDATE, True),
            # Contributors can update any ExperimentSet they contribute to
            PermissionTest("ExperimentSet", "private", "contributor", Action.UPDATE, True),
            PermissionTest("ExperimentSet", "published", "contributor", Action.UPDATE, True),
            # Mappers cannot update ExperimentSets
            PermissionTest("ExperimentSet", "private", "mapper", Action.UPDATE, False, 404),
            PermissionTest("ExperimentSet", "published", "mapper", Action.UPDATE, False, 403),
            # Other users cannot update ExperimentSets
            PermissionTest("ExperimentSet", "private", "other_user", Action.UPDATE, False, 404),
            PermissionTest("ExperimentSet", "published", "other_user", Action.UPDATE, False, 403),
            # Anonymous users cannot update ExperimentSets
            PermissionTest("ExperimentSet", "private", "anonymous", Action.UPDATE, False, 404),
            PermissionTest("ExperimentSet", "published", "anonymous", Action.UPDATE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        assert test_case.entity_state is not None, "ExperimentSet tests must have entity_state"
        experiment_set = entity_helper.create_experiment_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(
            user_data, experiment_set, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentSetDeleteActionHandler:
    """Test the _handle_delete_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can delete any ExperimentSet
            PermissionTest("ExperimentSet", "private", "admin", Action.DELETE, True),
            PermissionTest("ExperimentSet", "published", "admin", Action.DELETE, True),
            # Owners can only delete unpublished ExperimentSets
            PermissionTest("ExperimentSet", "private", "owner", Action.DELETE, True),
            PermissionTest("ExperimentSet", "published", "owner", Action.DELETE, False, 403),
            # Contributors cannot delete
            PermissionTest("ExperimentSet", "private", "contributor", Action.DELETE, False, 403),
            PermissionTest("ExperimentSet", "published", "contributor", Action.DELETE, False, 403),
            # Other users cannot delete
            PermissionTest("ExperimentSet", "private", "other_user", Action.DELETE, False, 404),
            PermissionTest("ExperimentSet", "published", "other_user", Action.DELETE, False, 403),
            # Anonymous users cannot delete
            PermissionTest("ExperimentSet", "private", "anonymous", Action.DELETE, False, 404),
            PermissionTest("ExperimentSet", "published", "anonymous", Action.DELETE, False, 401),
            # Mappers cannot delete
            PermissionTest("ExperimentSet", "private", "mapper", Action.DELETE, False, 404),
            PermissionTest("ExperimentSet", "published", "mapper", Action.DELETE, False, 403),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_delete_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_delete_action helper function directly."""
        assert test_case.entity_state is not None, "ExperimentSet tests must have entity_state"
        experiment_set = entity_helper.create_experiment_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_delete_action(
            user_data, experiment_set, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentSetAddExperimentActionHandler:
    """Test the _handle_add_experiment_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can add experiments to any ExperimentSet
            PermissionTest("ExperimentSet", "private", "admin", Action.ADD_EXPERIMENT, True),
            PermissionTest("ExperimentSet", "published", "admin", Action.ADD_EXPERIMENT, True),
            # Owners can add experiments to any ExperimentSet they own
            PermissionTest("ExperimentSet", "private", "owner", Action.ADD_EXPERIMENT, True),
            PermissionTest("ExperimentSet", "published", "owner", Action.ADD_EXPERIMENT, True),
            # Contributors can add experiments to any ExperimentSet they contribute to
            PermissionTest("ExperimentSet", "private", "contributor", Action.ADD_EXPERIMENT, True),
            PermissionTest("ExperimentSet", "published", "contributor", Action.ADD_EXPERIMENT, True),
            # Mappers cannot add experiments to ExperimentSets
            PermissionTest("ExperimentSet", "private", "mapper", Action.ADD_EXPERIMENT, False, 404),
            PermissionTest("ExperimentSet", "published", "mapper", Action.ADD_EXPERIMENT, False, 403),
            # Other users cannot add experiments to ExperimentSets
            PermissionTest("ExperimentSet", "private", "other_user", Action.ADD_EXPERIMENT, False, 404),
            PermissionTest("ExperimentSet", "published", "other_user", Action.ADD_EXPERIMENT, False, 403),
            # Anonymous users cannot add experiments to ExperimentSets
            PermissionTest("ExperimentSet", "private", "anonymous", Action.ADD_EXPERIMENT, False, 404),
            PermissionTest("ExperimentSet", "published", "anonymous", Action.ADD_EXPERIMENT, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_experiment_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_experiment_action helper function directly."""
        assert test_case.entity_state is not None, "ExperimentSet tests must have entity_state"
        experiment_set = entity_helper.create_experiment_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_experiment_action(
            user_data, experiment_set, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentSetDenyActionHandler:
    """Test experiment set deny action handler."""

    def test_deny_action_for_private_experiment_set_non_contributor(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_experiment_set helper function for private ExperimentSet."""
        experiment_set = entity_helper.create_experiment_set("private")

        # Private entity should return 404
        result = _deny_action_for_experiment_set(
            experiment_set, True, entity_helper.create_user_data("other_user"), False
        )
        assert result.permitted is False
        assert result.http_code == 404

    def test_deny_action_for_private_experiment_set_contributor(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_experiment_set helper function for private ExperimentSet with contributor user."""
        experiment_set = entity_helper.create_experiment_set("private")

        # Private entity, contributor user should return 404
        result = _deny_action_for_experiment_set(
            experiment_set, True, entity_helper.create_user_data("contributor"), True
        )
        assert result.permitted is False
        assert result.http_code == 403

    def test_deny_action_for_public_experiment_set_anonymous_user(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_experiment_set helper function for public ExperimentSet with anonymous user."""
        experiment_set = entity_helper.create_experiment_set("published")

        # Public entity, anonymous user should return 401
        result = _deny_action_for_experiment_set(experiment_set, False, None, False)
        assert result.permitted is False
        assert result.http_code == 401

    def test_deny_action_for_public_experiment_set_authenticated_user(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_experiment_set helper function for public ExperimentSet with authenticated user."""
        experiment_set = entity_helper.create_experiment_set("published")

        # Public entity, authenticated user should return 403
        result = _deny_action_for_experiment_set(
            experiment_set, False, entity_helper.create_user_data("other_user"), False
        )
        assert result.permitted is False
        assert result.http_code == 403
