# ruff: noqa: E402

"""Tests for ScoreSet permissions module."""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from typing import Callable, List
from unittest import mock

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.score_set import (
    _handle_delete_action,
    _handle_publish_action,
    _handle_read_action,
    _handle_set_scores_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

SCORE_SET_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.DELETE: _handle_delete_action,
    Action.SET_SCORES: _handle_set_scores_action,
    Action.PUBLISH: _handle_publish_action,
}

SCORE_SET_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.ADD_EXPERIMENT,
    Action.ADD_SCORE_SET,
    Action.ADD_ROLE,
    Action.LOOKUP,
    Action.ADD_BADGE,
    Action.CHANGE_RANK,
]


def test_score_set_handles_all_actions() -> None:
    """Test that all ScoreSet actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(SCORE_SET_SUPPORTED_ACTIONS)
    unsupported = set(SCORE_SET_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for score sets."


class TestScoreSetHasPermission:
    """Test the main has_permission dispatcher function for ScoreSet entities."""

    @pytest.mark.parametrize("action, handler", SCORE_SET_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        score_set = entity_helper.create_score_set()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.score_set." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, score_set, action)
            mock_handler.assert_called_once_with(
                admin_user,
                score_set,
                score_set.private,
                False,  # admin is not the owner
                False,  # admin is not a contributor
                [UserRole.admin],
            )

    @pytest.mark.parametrize("action", SCORE_SET_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        score_set = entity_helper.create_score_set()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, score_set, action)

        error_msg = str(exc_info.value)
        assert action.value in error_msg
        assert all(a.value in error_msg for a in SCORE_SET_SUPPORTED_ACTIONS)

    def test_requires_private_attribute(self, entity_helper: EntityTestHelper) -> None:
        """Test that ValueError is raised if ScoreSet.private is None."""
        score_set = entity_helper.create_score_set()
        score_set.private = None
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(ValueError) as exc_info:
            has_permission(admin_user, score_set, Action.READ)

        assert "private" in str(exc_info.value)


class TestScoreSetReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can read any ScoreSet
            PermissionTest("ScoreSet", "published", "admin", Action.READ, True),
            PermissionTest("ScoreSet", "private", "admin", Action.READ, True),
            # Owners can read any ScoreSet they own
            PermissionTest("ScoreSet", "published", "owner", Action.READ, True),
            PermissionTest("ScoreSet", "private", "owner", Action.READ, True),
            # Contributors can read any ScoreSet they contribute to
            PermissionTest("ScoreSet", "published", "contributor", Action.READ, True),
            PermissionTest("ScoreSet", "private", "contributor", Action.READ, True),
            # Mappers can read any ScoreSet (including private)
            PermissionTest("ScoreSet", "published", "mapper", Action.READ, True),
            PermissionTest("ScoreSet", "private", "mapper", Action.READ, True),
            # Other users can only read published ScoreSets
            PermissionTest("ScoreSet", "published", "other_user", Action.READ, True),
            PermissionTest("ScoreSet", "private", "other_user", Action.READ, False, 404),
            # Anonymous users can only read published ScoreSets
            PermissionTest("ScoreSet", "published", "anonymous", Action.READ, True),
            PermissionTest("ScoreSet", "private", "anonymous", Action.READ, False, 404),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreSet tests must have entity_state"
        score_set = entity_helper.create_score_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(user_data, score_set, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreSetUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can update any ScoreSet
            PermissionTest("ScoreSet", "private", "admin", Action.UPDATE, True),
            PermissionTest("ScoreSet", "published", "admin", Action.UPDATE, True),
            # Owners can update any ScoreSet they own
            PermissionTest("ScoreSet", "private", "owner", Action.UPDATE, True),
            PermissionTest("ScoreSet", "published", "owner", Action.UPDATE, True),
            # Contributors can update any ScoreSet they contribute to
            PermissionTest("ScoreSet", "private", "contributor", Action.UPDATE, True),
            PermissionTest("ScoreSet", "published", "contributor", Action.UPDATE, True),
            # Mappers cannot update ScoreSets
            PermissionTest("ScoreSet", "private", "mapper", Action.UPDATE, False, 404),
            PermissionTest("ScoreSet", "published", "mapper", Action.UPDATE, False, 403),
            # Other users cannot update ScoreSets
            PermissionTest("ScoreSet", "private", "other_user", Action.UPDATE, False, 404),
            PermissionTest("ScoreSet", "published", "other_user", Action.UPDATE, False, 403),
            # Anonymous users cannot update ScoreSets
            PermissionTest("ScoreSet", "private", "anonymous", Action.UPDATE, False, 404),
            PermissionTest("ScoreSet", "published", "anonymous", Action.UPDATE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreSet tests must have entity_state"
        score_set = entity_helper.create_score_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(user_data, score_set, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreSetDeleteActionHandler:
    """Test the _handle_delete_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can delete any ScoreSet
            PermissionTest("ScoreSet", "private", "admin", Action.DELETE, True),
            PermissionTest("ScoreSet", "published", "admin", Action.DELETE, True),
            # Owners can only delete unpublished ScoreSets
            PermissionTest("ScoreSet", "private", "owner", Action.DELETE, True),
            PermissionTest("ScoreSet", "published", "owner", Action.DELETE, False, 403),
            # Contributors cannot delete
            PermissionTest("ScoreSet", "private", "contributor", Action.DELETE, False, 403),
            PermissionTest("ScoreSet", "published", "contributor", Action.DELETE, False, 403),
            # Other users cannot delete
            PermissionTest("ScoreSet", "private", "other_user", Action.DELETE, False, 404),
            PermissionTest("ScoreSet", "published", "other_user", Action.DELETE, False, 403),
            # Anonymous users cannot delete
            PermissionTest("ScoreSet", "private", "anonymous", Action.DELETE, False, 404),
            PermissionTest("ScoreSet", "published", "anonymous", Action.DELETE, False, 401),
            # Mappers cannot delete
            PermissionTest("ScoreSet", "private", "mapper", Action.DELETE, False, 404),
            PermissionTest("ScoreSet", "published", "mapper", Action.DELETE, False, 403),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_delete_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_delete_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreSet tests must have entity_state"
        score_set = entity_helper.create_score_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_delete_action(user_data, score_set, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreSetSetScoresActionHandler:
    """Test the _handle_set_scores_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can set scores on any ScoreSet
            PermissionTest("ScoreSet", "private", "admin", Action.SET_SCORES, True),
            PermissionTest("ScoreSet", "published", "admin", Action.SET_SCORES, True),
            # Owners can set scores on any ScoreSet they own
            PermissionTest("ScoreSet", "private", "owner", Action.SET_SCORES, True),
            PermissionTest("ScoreSet", "published", "owner", Action.SET_SCORES, True),
            # Contributors can set scores on any ScoreSet they contribute to
            PermissionTest("ScoreSet", "private", "contributor", Action.SET_SCORES, True),
            PermissionTest("ScoreSet", "published", "contributor", Action.SET_SCORES, True),
            # Mappers cannot set scores on ScoreSets
            PermissionTest("ScoreSet", "private", "mapper", Action.SET_SCORES, False, 404),
            PermissionTest("ScoreSet", "published", "mapper", Action.SET_SCORES, False, 403),
            # Other users cannot set scores on ScoreSets
            PermissionTest("ScoreSet", "private", "other_user", Action.SET_SCORES, False, 404),
            PermissionTest("ScoreSet", "published", "other_user", Action.SET_SCORES, False, 403),
            # Anonymous users cannot set scores on ScoreSets
            PermissionTest("ScoreSet", "private", "anonymous", Action.SET_SCORES, False, 404),
            PermissionTest("ScoreSet", "published", "anonymous", Action.SET_SCORES, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_set_scores_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_set_scores_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreSet tests must have entity_state"
        score_set = entity_helper.create_score_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_set_scores_action(
            user_data, score_set, private, user_is_owner, user_is_contributor, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreSetPublishActionHandler:
    """Test the _handle_publish_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # Admins can publish any ScoreSet
            PermissionTest("ScoreSet", "private", "admin", Action.PUBLISH, True),
            PermissionTest("ScoreSet", "published", "admin", Action.PUBLISH, True),
            # Owners can publish any ScoreSet they own
            PermissionTest("ScoreSet", "private", "owner", Action.PUBLISH, True),
            PermissionTest("ScoreSet", "published", "owner", Action.PUBLISH, True),
            # Contributors cannot publish ScoreSets they contribute to
            PermissionTest("ScoreSet", "private", "contributor", Action.PUBLISH, False, 403),
            PermissionTest("ScoreSet", "published", "contributor", Action.PUBLISH, False, 403),
            # Mappers cannot publish ScoreSets
            PermissionTest("ScoreSet", "private", "mapper", Action.PUBLISH, False, 404),
            PermissionTest("ScoreSet", "published", "mapper", Action.PUBLISH, False, 403),
            # Other users cannot publish ScoreSets
            PermissionTest("ScoreSet", "private", "other_user", Action.PUBLISH, False, 404),
            PermissionTest("ScoreSet", "published", "other_user", Action.PUBLISH, False, 403),
            # Anonymous users cannot publish ScoreSets
            PermissionTest("ScoreSet", "private", "anonymous", Action.PUBLISH, False, 404),
            PermissionTest("ScoreSet", "published", "anonymous", Action.PUBLISH, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_publish_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_publish_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreSet tests must have entity_state"
        score_set = entity_helper.create_score_set(test_case.entity_state)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_publish_action(user_data, score_set, private, user_is_owner, user_is_contributor, active_roles)

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code
