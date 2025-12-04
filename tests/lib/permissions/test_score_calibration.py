"""Tests for ScoreCalibration permissions module."""

from typing import Callable, List
from unittest import mock

import pytest

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.score_calibration import (
    _deny_action_for_score_calibration,
    _handle_change_rank_action,
    _handle_delete_action,
    _handle_publish_action,
    _handle_read_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

SCORE_CALIBRATION_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.DELETE: _handle_delete_action,
    Action.PUBLISH: _handle_publish_action,
    Action.CHANGE_RANK: _handle_change_rank_action,
}

SCORE_CALIBRATION_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.ADD_EXPERIMENT,
    Action.ADD_SCORE_SET,
    Action.ADD_ROLE,
    Action.LOOKUP,
    Action.ADD_BADGE,
    Action.SET_SCORES,
]


def test_score_calibration_handles_all_actions() -> None:
    """Test that all ScoreCalibration actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(SCORE_CALIBRATION_SUPPORTED_ACTIONS)
    unsupported = set(SCORE_CALIBRATION_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for score calibrations."


class TestScoreCalibrationHasPermission:
    """Test the main has_permission dispatcher function for ScoreCalibration entities."""

    @pytest.mark.parametrize("action, handler", SCORE_CALIBRATION_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        score_calibration = entity_helper.create_score_calibration()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.score_calibration." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, score_calibration, action)
            mock_handler.assert_called_once_with(
                admin_user,
                score_calibration,
                False,  # admin is not the owner
                False,  # admin is not a contributor to score set
                score_calibration.private,
                [UserRole.admin],
            )

    @pytest.mark.parametrize("action", SCORE_CALIBRATION_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        score_calibration = entity_helper.create_score_calibration()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, score_calibration, action)

        error_msg = str(exc_info.value)
        assert action.value in error_msg
        assert all(a.value in error_msg for a in SCORE_CALIBRATION_SUPPORTED_ACTIONS)

    def test_requires_private_attribute(self, entity_helper: EntityTestHelper) -> None:
        """Test that ValueError is raised if ScoreCalibration.private is None."""
        score_calibration = entity_helper.create_score_calibration()
        score_calibration.private = None
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(ValueError) as exc_info:
            has_permission(admin_user, score_calibration, Action.READ)

        assert "private" in str(exc_info.value)


class TestScoreCalibrationReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins: Can read any ScoreCalibration regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "published", "admin", Action.READ, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "admin", Action.READ, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "private", "admin", Action.READ, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "admin", Action.READ, True, investigator_provided=False),
            # Owners: Can read any ScoreCalibration they created regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "published", "owner", Action.READ, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "owner", Action.READ, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "private", "owner", Action.READ, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "owner", Action.READ, True, investigator_provided=False),
            # Contributors to associated ScoreSet: Can read published ScoreCalibrations (any type) and private investigator-provided ScoreCalibrations, but NOT private community-provided ones
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.READ, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.READ, True, investigator_provided=False
            ),
            PermissionTest("ScoreCalibration", "private", "contributor", Action.READ, True, investigator_provided=True),
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.READ, False, 404, investigator_provided=False
            ),
            # Other users: Can only read published ScoreCalibrations, cannot access any private ones
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.READ, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.READ, True, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.READ, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.READ, False, 404, investigator_provided=False
            ),
            # Anonymous users: Can only read published ScoreCalibrations, cannot access any private ones
            PermissionTest("ScoreCalibration", "published", "anonymous", Action.READ, True, investigator_provided=True),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.READ, True, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.READ, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.READ, False, 404, investigator_provided=False
            ),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{'investigator' if tc.investigator_provided else 'community'}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreCalibration tests must have entity_state"
        assert test_case.investigator_provided is not None, "ScoreCalibration tests must have investigator_provided"
        score_calibration = entity_helper.create_score_calibration(
            test_case.entity_state, test_case.investigator_provided
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor_to_score_set = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(
            user_data, score_calibration, user_is_owner, user_is_contributor_to_score_set, private, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreCalibrationUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins: Can update any ScoreCalibration regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "private", "admin", Action.UPDATE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "admin", Action.UPDATE, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "published", "admin", Action.UPDATE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "admin", Action.UPDATE, True, investigator_provided=False),
            # Owners: Can update only their own private ScoreCalibrations, cannot update published ones (even their own)
            PermissionTest("ScoreCalibration", "private", "owner", Action.UPDATE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "owner", Action.UPDATE, True, investigator_provided=False),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.UPDATE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.UPDATE, False, 403, investigator_provided=False
            ),
            # Contributors to associated ScoreSet: Can update only private investigator-provided ScoreCalibrations, cannot update community-provided or published ones
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.UPDATE, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.UPDATE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.UPDATE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.UPDATE, False, 403, investigator_provided=False
            ),
            # Other users: Cannot update any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.UPDATE, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.UPDATE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.UPDATE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.UPDATE, False, 403, investigator_provided=False
            ),
            # Anonymous users: Cannot update any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.UPDATE, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.UPDATE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.UPDATE, False, 401, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.UPDATE, False, 401, investigator_provided=False
            ),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{'investigator' if tc.investigator_provided else 'community'}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreCalibration tests must have entity_state"
        assert test_case.investigator_provided is not None, "ScoreCalibration tests must have investigator_provided"
        score_calibration = entity_helper.create_score_calibration(
            test_case.entity_state, test_case.investigator_provided
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor_to_score_set = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(
            user_data, score_calibration, user_is_owner, user_is_contributor_to_score_set, private, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreCalibrationDeleteActionHandler:
    """Test the _handle_delete_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins: Can delete any ScoreCalibration regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "private", "admin", Action.DELETE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "admin", Action.DELETE, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "published", "admin", Action.DELETE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "admin", Action.DELETE, True, investigator_provided=False),
            # Owners: Can delete only their own private ScoreCalibrations, cannot delete published ones (even their own)
            PermissionTest("ScoreCalibration", "private", "owner", Action.DELETE, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "owner", Action.DELETE, True, investigator_provided=False),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.DELETE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.DELETE, False, 403, investigator_provided=False
            ),
            # Contributors to associated ScoreSet: Cannot delete any ScoreCalibrations (even investigator-provided ones they can read/update)
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.DELETE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.DELETE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.DELETE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.DELETE, False, 403, investigator_provided=False
            ),
            # Other users: Cannot delete any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.DELETE, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.DELETE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.DELETE, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.DELETE, False, 403, investigator_provided=False
            ),
            # Anonymous users: Cannot delete any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.DELETE, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.DELETE, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.DELETE, False, 401, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.DELETE, False, 401, investigator_provided=False
            ),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{'investigator' if tc.investigator_provided else 'community'}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_delete_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_delete_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreCalibration tests must have entity_state"
        assert test_case.investigator_provided is not None, "ScoreCalibration tests must have investigator_provided"
        score_calibration = entity_helper.create_score_calibration(
            test_case.entity_state, test_case.investigator_provided
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor_to_score_set = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_delete_action(
            user_data, score_calibration, user_is_owner, user_is_contributor_to_score_set, private, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreCalibrationPublishActionHandler:
    """Test the _handle_publish_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins: Can publish any ScoreCalibration regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "private", "admin", Action.PUBLISH, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "admin", Action.PUBLISH, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "published", "admin", Action.PUBLISH, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "admin", Action.PUBLISH, True, investigator_provided=False),
            # Owners: Can publish their own ScoreCalibrations regardless of state or investigator_provided flag
            PermissionTest("ScoreCalibration", "private", "owner", Action.PUBLISH, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "private", "owner", Action.PUBLISH, True, investigator_provided=False),
            PermissionTest("ScoreCalibration", "published", "owner", Action.PUBLISH, True, investigator_provided=True),
            PermissionTest("ScoreCalibration", "published", "owner", Action.PUBLISH, True, investigator_provided=False),
            # Contributors to associated ScoreSet: Cannot publish any ScoreCalibrations (even investigator-provided ones they can read/update)
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.PUBLISH, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.PUBLISH, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.PUBLISH, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.PUBLISH, False, 403, investigator_provided=False
            ),
            # Other users: Cannot publish any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.PUBLISH, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.PUBLISH, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.PUBLISH, False, 403, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "other_user", Action.PUBLISH, False, 403, investigator_provided=False
            ),
            # Anonymous users: Cannot publish any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.PUBLISH, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.PUBLISH, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.PUBLISH, False, 401, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.PUBLISH, False, 401, investigator_provided=False
            ),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{'investigator' if tc.investigator_provided else 'community'}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_publish_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_publish_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreCalibration tests must have entity_state"
        assert test_case.investigator_provided is not None, "ScoreCalibration tests must have investigator_provided"
        score_calibration = entity_helper.create_score_calibration(
            test_case.entity_state, test_case.investigator_provided
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor_to_score_set = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_publish_action(
            user_data, score_calibration, user_is_owner, user_is_contributor_to_score_set, private, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreCalibrationChangeRankActionHandler:
    """Test the _handle_change_rank_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins: Can change rank of any ScoreCalibration regardless of state or investigator_provided flag
            PermissionTest(
                "ScoreCalibration", "private", "admin", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "admin", Action.CHANGE_RANK, True, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "admin", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "admin", Action.CHANGE_RANK, True, investigator_provided=False
            ),
            # Owners: Can change rank of their own ScoreCalibrations regardless of state or investigator_provided flag
            PermissionTest(
                "ScoreCalibration", "private", "owner", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "owner", Action.CHANGE_RANK, True, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "published", "owner", Action.CHANGE_RANK, True, investigator_provided=False
            ),
            # Contributors to associated ScoreSet: Can change rank of investigator-provided ScoreCalibrations (private or published), but cannot change rank of community-provided ones
            PermissionTest(
                "ScoreCalibration", "private", "contributor", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.CHANGE_RANK,
                False,
                404,
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration", "published", "contributor", Action.CHANGE_RANK, True, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "contributor",
                Action.CHANGE_RANK,
                False,
                403,
                investigator_provided=False,
            ),
            # Other users: Cannot change rank of any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.CHANGE_RANK, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "other_user", Action.CHANGE_RANK, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "other_user",
                Action.CHANGE_RANK,
                False,
                403,
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "other_user",
                Action.CHANGE_RANK,
                False,
                403,
                investigator_provided=False,
            ),
            # Anonymous users: Cannot change rank of any ScoreCalibrations
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.CHANGE_RANK, False, 404, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration", "private", "anonymous", Action.CHANGE_RANK, False, 404, investigator_provided=False
            ),
            PermissionTest(
                "ScoreCalibration", "published", "anonymous", Action.CHANGE_RANK, False, 401, investigator_provided=True
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "anonymous",
                Action.CHANGE_RANK,
                False,
                401,
                investigator_provided=False,
            ),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.entity_state}_{'investigator' if tc.investigator_provided else 'community'}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_change_rank_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_change_rank_action helper function directly."""
        assert test_case.entity_state is not None, "ScoreCalibration tests must have entity_state"
        assert test_case.investigator_provided is not None, "ScoreCalibration tests must have investigator_provided"
        score_calibration = entity_helper.create_score_calibration(
            test_case.entity_state, test_case.investigator_provided
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        user_is_owner = test_case.user_type == "owner"
        user_is_contributor_to_score_set = test_case.user_type == "contributor"
        active_roles = user_data.active_roles if user_data else []

        result = _handle_change_rank_action(
            user_data, score_calibration, user_is_owner, user_is_contributor_to_score_set, private, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestScoreCalibrationDenyActionHandler:
    """Test score calibration deny action handler."""

    def test_deny_action_for_private_score_calibration_not_contributor(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_score_calibration helper function for private ScoreCalibration."""
        score_calibration = entity_helper.create_score_calibration("private", True)

        # Private entity should return 404
        result = _deny_action_for_score_calibration(
            score_calibration, True, entity_helper.create_user_data("other_user"), False
        )
        assert result.permitted is False
        assert result.http_code == 404

    def test_deny_action_for_public_score_calibration_anonymous_user(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_score_calibration helper function for public ScoreCalibration with anonymous user."""
        score_calibration = entity_helper.create_score_calibration("published", True)

        # Public entity, anonymous user should return 401
        result = _deny_action_for_score_calibration(score_calibration, False, None, False)
        assert result.permitted is False
        assert result.http_code == 401

    def test_deny_action_for_public_score_calibration_authenticated_user(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_score_calibration helper function for public ScoreCalibration with authenticated user."""
        score_calibration = entity_helper.create_score_calibration("published", True)

        # Public entity, authenticated user should return 403
        result = _deny_action_for_score_calibration(
            score_calibration, False, entity_helper.create_user_data("other_user"), False
        )
        assert result.permitted is False
        assert result.http_code == 403

    def test_deny_action_for_private_score_calibration_with_contributor(self, entity_helper: EntityTestHelper) -> None:
        """Test _deny_action_for_score_calibration helper function for private ScoreCalibration with contributor user."""
        score_calibration = entity_helper.create_score_calibration("private", True)

        # Private entity with contributor user should return 403
        result = _deny_action_for_score_calibration(
            score_calibration, True, entity_helper.create_user_data("contributor"), True
        )
        assert result.permitted is False
        assert result.http_code == 403
