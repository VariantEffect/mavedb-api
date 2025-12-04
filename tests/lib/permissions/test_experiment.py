# ruff: noqa: E402

"""Tests for Experiment permissions module."""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from typing import Callable, List
from unittest import mock

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.experiment import (
    _handle_add_score_set_action,
    _handle_delete_action,
    _handle_read_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

EXPERIMENT_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.DELETE: _handle_delete_action,
    Action.ADD_SCORE_SET: _handle_add_score_set_action,
}

EXPERIMENT_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.ADD_EXPERIMENT,
    Action.ADD_ROLE,
    Action.LOOKUP,
    Action.ADD_BADGE,
    Action.CHANGE_RANK,
    Action.SET_SCORES,
    Action.PUBLISH,
]


def test_experiment_handles_all_actions() -> None:
    """Test that all Experiment actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(EXPERIMENT_SUPPORTED_ACTIONS)
    unsupported = set(EXPERIMENT_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for experiments."


class TestExperimentHasPermission:
    """Test the main has_permission dispatcher function for Experiment entities."""

    @pytest.mark.parametrize("action, handler", EXPERIMENT_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        experiment = entity_helper.create_experiment()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.experiment." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, experiment, action)
            mock_handler.assert_called_once_with(
                admin_user,
                experiment,
                experiment.private,
                False,  # admin is not the owner
                False,  # admin is not a contributor
                [UserRole.admin],
            )

    @pytest.mark.parametrize("action", EXPERIMENT_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        experiment = entity_helper.create_experiment()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, experiment, action)

        error_msg = str(exc_info.value)
        assert action.value in error_msg
        assert all(a.value in error_msg for a in EXPERIMENT_SUPPORTED_ACTIONS)

    def test_requires_private_attribute(self, entity_helper: EntityTestHelper) -> None:
        """Test that ValueError is raised if Experiment.private is None."""
        experiment = entity_helper.create_experiment()
        experiment.private = None
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(ValueError) as exc_info:
            has_permission(admin_user, experiment, Action.READ)

        assert "private" in str(exc_info.value)


class TestExperimentReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can read any Experiment
            PermissionTest("Experiment", "published", "admin", Action.READ, True),
            PermissionTest("Experiment", "private", "admin", Action.READ, True),
            # Owners can read any Experiment they own
            PermissionTest("Experiment", "published", "owner", Action.READ, True),
            PermissionTest("Experiment", "private", "owner", Action.READ, True),
            # Contributors can read any Experiment they contribute to
            PermissionTest("Experiment", "published", "contributor", Action.READ, True),
            PermissionTest("Experiment", "private", "contributor", Action.READ, True),
            # Mappers can read any Experiment (including private)
            PermissionTest("Experiment", "published", "mapper", Action.READ, True),
            PermissionTest("Experiment", "private", "mapper", Action.READ, True),
            # Other users can only read published Experiments
            PermissionTest("Experiment", "published", "other_user", Action.READ, True),
            PermissionTest("Experiment", "private", "other_user", Action.READ, False, 404),
            # Anonymous users can only read published Experiments
            PermissionTest("Experiment", "published", "anonymous", Action.READ, True),
            PermissionTest("Experiment", "private", "anonymous", Action.READ, False, 404),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        assert test_case.entity_state is not None, "Experiment tests must have entity_state"
        experiment = entity_helper.create_experiment(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(user_data, experiment, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can update any Experiment
            PermissionTest("Experiment", "private", "admin", Action.UPDATE, True),
            PermissionTest("Experiment", "published", "admin", Action.UPDATE, True),
            # Owners can update any Experiment they own
            PermissionTest("Experiment", "private", "owner", Action.UPDATE, True),
            PermissionTest("Experiment", "published", "owner", Action.UPDATE, True),
            # Contributors can update any Experiment they contribute to
            PermissionTest("Experiment", "private", "contributor", Action.UPDATE, True),
            PermissionTest("Experiment", "published", "contributor", Action.UPDATE, True),
            # Mappers cannot update Experiments
            PermissionTest("Experiment", "private", "mapper", Action.UPDATE, False, 404),
            PermissionTest("Experiment", "published", "mapper", Action.UPDATE, False, 403),
            # Other users cannot update Experiments
            PermissionTest("Experiment", "private", "other_user", Action.UPDATE, False, 404),
            PermissionTest("Experiment", "published", "other_user", Action.UPDATE, False, 403),
            # Anonymous users cannot update Experiments
            PermissionTest("Experiment", "private", "anonymous", Action.UPDATE, False, 404),
            PermissionTest("Experiment", "published", "anonymous", Action.UPDATE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        assert test_case.entity_state is not None, "Experiment tests must have entity_state"
        experiment = entity_helper.create_experiment(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(user_data, experiment, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentDeleteActionHandler:
    """Test the _handle_delete_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can delete any Experiment
            PermissionTest("Experiment", "private", "admin", Action.DELETE, True),
            PermissionTest("Experiment", "published", "admin", Action.DELETE, True),
            # Owners can only delete unpublished Experiments
            PermissionTest("Experiment", "private", "owner", Action.DELETE, True),
            PermissionTest("Experiment", "published", "owner", Action.DELETE, False, 403),
            # Contributors cannot delete
            PermissionTest("Experiment", "private", "contributor", Action.DELETE, False, 403),
            PermissionTest("Experiment", "published", "contributor", Action.DELETE, False, 403),
            # Other users cannot delete
            PermissionTest("Experiment", "private", "other_user", Action.DELETE, False, 404),
            PermissionTest("Experiment", "published", "other_user", Action.DELETE, False, 403),
            # Anonymous users cannot delete
            PermissionTest("Experiment", "private", "anonymous", Action.DELETE, False, 404),
            PermissionTest("Experiment", "published", "anonymous", Action.DELETE, False, 401),
            # Mappers cannot delete
            PermissionTest("Experiment", "private", "mapper", Action.DELETE, False, 404),
            PermissionTest("Experiment", "published", "mapper", Action.DELETE, False, 403),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_delete_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_delete_action helper function directly."""
        assert test_case.entity_state is not None, "Experiment tests must have entity_state"
        experiment = entity_helper.create_experiment(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_delete_action(user_data, experiment, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestExperimentAddScoreSetActionHandler:
    """Test the _handle_add_score_set_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can add score sets to any Experiment
            PermissionTest("Experiment", "private", "admin", Action.ADD_SCORE_SET, True),
            PermissionTest("Experiment", "published", "admin", Action.ADD_SCORE_SET, True),
            # Owners can add score sets to any Experiment they own
            PermissionTest("Experiment", "private", "owner", Action.ADD_SCORE_SET, True),
            PermissionTest("Experiment", "published", "owner", Action.ADD_SCORE_SET, True),
            # Contributors can add score sets to any Experiment they contribute to
            PermissionTest("Experiment", "private", "contributor", Action.ADD_SCORE_SET, True),
            PermissionTest("Experiment", "published", "contributor", Action.ADD_SCORE_SET, True),
            # Mappers can add score sets to public Experiments
            PermissionTest("Experiment", "private", "mapper", Action.ADD_SCORE_SET, False, 404),
            PermissionTest("Experiment", "published", "mapper", Action.ADD_SCORE_SET, True),
            # Other users can add score sets to public Experiments
            PermissionTest("Experiment", "private", "other_user", Action.ADD_SCORE_SET, False, 404),
            PermissionTest("Experiment", "published", "other_user", Action.ADD_SCORE_SET, True),
            # Anonymous users cannot add score sets to Experiments
            PermissionTest("Experiment", "private", "anonymous", Action.ADD_SCORE_SET, False, 404),
            PermissionTest("Experiment", "published", "anonymous", Action.ADD_SCORE_SET, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_score_set_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_score_set_action helper function directly."""
        assert test_case.entity_state is not None, "Experiment tests must have entity_state"
        experiment = entity_helper.create_experiment(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_score_set_action(
            user_data, experiment, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code
