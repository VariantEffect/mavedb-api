# ruff: noqa: E402

"""Tests for Collection permissions module."""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from typing import Callable, List
from unittest import mock

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.collection import (
    _handle_add_badge_action,
    _handle_add_experiment_action,
    _handle_add_role_action,
    _handle_add_score_set_action,
    _handle_delete_action,
    _handle_publish_action,
    _handle_read_action,
    _handle_update_action,
    has_permission,
)
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole
from tests.lib.permissions.conftest import EntityTestHelper, PermissionTest

COLLECTION_SUPPORTED_ACTIONS: dict[Action, Callable] = {
    Action.READ: _handle_read_action,
    Action.UPDATE: _handle_update_action,
    Action.DELETE: _handle_delete_action,
    Action.PUBLISH: _handle_publish_action,
    Action.ADD_EXPERIMENT: _handle_add_experiment_action,
    Action.ADD_SCORE_SET: _handle_add_score_set_action,
    Action.ADD_ROLE: _handle_add_role_action,
    Action.ADD_BADGE: _handle_add_badge_action,
}

COLLECTION_UNSUPPORTED_ACTIONS: List[Action] = [
    Action.LOOKUP,
    Action.CHANGE_RANK,
    Action.SET_SCORES,
]

COLLECTION_ROLE_MAP = {
    "collection_admin": ContributionRole.admin,
    "collection_editor": ContributionRole.editor,
    "collection_viewer": ContributionRole.viewer,
}


def test_collection_handles_all_actions() -> None:
    """Test that all Collection actions are either supported or explicitly unsupported."""
    all_actions = set(action for action in Action)
    supported = set(COLLECTION_SUPPORTED_ACTIONS)
    unsupported = set(COLLECTION_UNSUPPORTED_ACTIONS)

    assert (
        supported.union(unsupported) == all_actions
    ), "Some actions are not categorized as supported or unsupported for collections."


class TestCollectionHasPermission:
    """Test the main has_permission dispatcher function for Collection entities."""

    @pytest.mark.parametrize("action, handler", COLLECTION_SUPPORTED_ACTIONS.items())
    def test_supported_actions_route_to_correct_action_handler(
        self, entity_helper: EntityTestHelper, action: Action, handler: Callable
    ) -> None:
        """Test that has_permission routes supported actions to their handlers."""
        collection = entity_helper.create_collection()
        admin_user = entity_helper.create_user_data("admin")

        with mock.patch("mavedb.lib.permissions.collection." + handler.__name__, wraps=handler) as mock_handler:
            has_permission(admin_user, collection, action)
            mock_handler.assert_called_once_with(
                admin_user,
                collection,
                collection.private,
                collection.badge_name is not None,
                False,  # admin is not the owner
                [],  # admin has no collection roles
                [UserRole.admin],
            )

    def test_has_permission_calls_helper_with_collection_roles_when_present(self, entity_helper: EntityTestHelper):
        """Test that has_permission passes collection roles to action handlers."""
        collection = entity_helper.create_collection(collection_role="collection_editor")
        contributor_user = entity_helper.create_user_data("contributor")

        with mock.patch(
            "mavedb.lib.permissions.collection._handle_read_action", wraps=_handle_read_action
        ) as mock_handler:
            has_permission(contributor_user, collection, Action.READ)
            mock_handler.assert_called_once_with(
                contributor_user,
                collection,
                collection.private,
                collection.badge_name is not None,
                False,  # contributor is not the owner
                [ContributionRole.editor],  # collection role
                [],  # user has no active roles
            )

    @pytest.mark.parametrize("action", COLLECTION_UNSUPPORTED_ACTIONS)
    def test_raises_for_unsupported_actions(self, entity_helper: EntityTestHelper, action: Action) -> None:
        """Test that unsupported actions raise NotImplementedError with descriptive message."""
        collection = entity_helper.create_collection()
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(NotImplementedError) as exc_info:
            has_permission(admin_user, collection, action)

        error_msg = str(exc_info.value)
        assert action.value in error_msg
        assert all(a.value in error_msg for a in COLLECTION_SUPPORTED_ACTIONS)

    def test_requires_private_attribute(self, entity_helper: EntityTestHelper) -> None:
        """Test that ValueError is raised if Collection.private is None."""
        collection = entity_helper.create_collection()
        collection.private = None
        admin_user = entity_helper.create_user_data("admin")

        with pytest.raises(ValueError) as exc_info:
            has_permission(admin_user, collection, Action.READ)

        assert "private" in str(exc_info.value)


class TestCollectionReadActionHandler:
    """Test the _handle_read_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can read any Collection
            PermissionTest("Collection", "published", "admin", Action.READ, True),
            PermissionTest("Collection", "private", "admin", Action.READ, True),
            # Owners can read any Collection they own
            PermissionTest("Collection", "published", "owner", Action.READ, True),
            PermissionTest("Collection", "private", "owner", Action.READ, True),
            # Collection admins can read any Collection they have admin role for
            PermissionTest(
                "Collection", "published", "contributor", Action.READ, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "private", "contributor", Action.READ, True, collection_role="collection_admin"
            ),
            # Collection editors can read any Collection they have editor role for
            PermissionTest(
                "Collection", "published", "contributor", Action.READ, True, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection", "private", "contributor", Action.READ, True, collection_role="collection_editor"
            ),
            # Collection viewers can read any Collection they have viewer role for
            PermissionTest(
                "Collection", "published", "contributor", Action.READ, True, collection_role="collection_viewer"
            ),
            PermissionTest(
                "Collection", "private", "contributor", Action.READ, True, collection_role="collection_viewer"
            ),
            # Other users can only read published Collections
            PermissionTest("Collection", "published", "other_user", Action.READ, True),
            PermissionTest("Collection", "private", "other_user", Action.READ, False, 404),
            # Anonymous users can only read published Collections
            PermissionTest("Collection", "published", "anonymous", Action.READ, True),
            PermissionTest("Collection", "private", "anonymous", Action.READ, False, 404),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_read_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_read_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        # Determine user relationship to entity
        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        # Test the helper function directly
        result = _handle_read_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionUpdateActionHandler:
    """Test the _handle_update_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can update any Collection
            PermissionTest("Collection", "private", "admin", Action.UPDATE, True),
            PermissionTest("Collection", "published", "admin", Action.UPDATE, True),
            # Owners can update any Collection they own
            PermissionTest("Collection", "private", "owner", Action.UPDATE, True),
            PermissionTest("Collection", "published", "owner", Action.UPDATE, True),
            # Collection admins can update any Collection they have admin role for
            PermissionTest(
                "Collection", "private", "contributor", Action.UPDATE, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.UPDATE, True, collection_role="collection_admin"
            ),
            # Collection editors can update any Collection they have editor role for
            PermissionTest(
                "Collection", "private", "contributor", Action.UPDATE, True, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.UPDATE, True, collection_role="collection_editor"
            ),
            # Collection viewers cannot update Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.UPDATE, False, 403, collection_role="collection_viewer"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.UPDATE, False, 403, collection_role="collection_viewer"
            ),
            # Other users cannot update Collections
            PermissionTest("Collection", "private", "other_user", Action.UPDATE, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.UPDATE, False, 403),
            # Anonymous users cannot update Collections
            PermissionTest("Collection", "private", "anonymous", Action.UPDATE, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.UPDATE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_update_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_update_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_update_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionDeleteActionHandler:
    """Test the _handle_delete_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can delete any Collection
            PermissionTest("Collection", "private", "admin", Action.DELETE, True),
            PermissionTest("Collection", "published", "admin", Action.DELETE, True),
            PermissionTest("Collection", "private", "admin", Action.DELETE, True, collection_badge="official"),
            PermissionTest("Collection", "published", "admin", Action.DELETE, True, collection_badge="official"),
            # Owners can only delete unpublished, unofficial Collections
            PermissionTest("Collection", "private", "owner", Action.DELETE, True),
            PermissionTest("Collection", "published", "owner", Action.DELETE, False, 403),
            PermissionTest("Collection", "private", "owner", Action.DELETE, False, 403, collection_badge="official"),
            PermissionTest("Collection", "published", "owner", Action.DELETE, False, 403, collection_badge="official"),
            # Collection admins cannot delete Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.DELETE, False, 403, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.DELETE, False, 403, collection_role="collection_admin"
            ),
            # Collection editors cannot delete Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.DELETE, False, 403, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.DELETE, False, 403, collection_role="collection_editor"
            ),
            # Collection viewers cannot delete Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.DELETE, False, 403, collection_role="collection_viewer"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.DELETE, False, 403, collection_role="collection_viewer"
            ),
            # Other users cannot delete Collections
            PermissionTest("Collection", "private", "other_user", Action.DELETE, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.DELETE, False, 403),
            # Anonymous users cannot delete Collections
            PermissionTest("Collection", "private", "anonymous", Action.DELETE, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.DELETE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_delete_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_delete_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(
            test_case.entity_state, collection_role=test_case.collection_role, badge_name=test_case.collection_badge
        )
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_delete_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionPublishActionHandler:
    """Test the _handle_publish_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can publish any Collection
            PermissionTest("Collection", "private", "admin", Action.PUBLISH, True),
            PermissionTest("Collection", "published", "admin", Action.PUBLISH, True),
            # Owners can publish any Collection they own
            PermissionTest("Collection", "private", "owner", Action.PUBLISH, True),
            PermissionTest("Collection", "published", "owner", Action.PUBLISH, True),
            # Collection admins can publish any Collection they have admin role for
            PermissionTest(
                "Collection", "private", "contributor", Action.PUBLISH, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.PUBLISH, True, collection_role="collection_admin"
            ),
            # Collection editors cannot publish Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.PUBLISH, False, 403, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.PUBLISH,
                False,
                403,
                collection_role="collection_editor",
            ),
            # Collection viewers cannot publish Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.PUBLISH, False, 403, collection_role="collection_viewer"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.PUBLISH,
                False,
                403,
                collection_role="collection_viewer",
            ),
            # Other users cannot publish Collections
            PermissionTest("Collection", "private", "other_user", Action.PUBLISH, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.PUBLISH, False, 403),
            # Anonymous users cannot publish Collections
            PermissionTest("Collection", "private", "anonymous", Action.PUBLISH, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.PUBLISH, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_publish_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_publish_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_publish_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionAddExperimentActionHandler:
    """Test the _handle_add_experiment_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can add experiments to any Collection
            PermissionTest("Collection", "private", "admin", Action.ADD_EXPERIMENT, True),
            PermissionTest("Collection", "published", "admin", Action.ADD_EXPERIMENT, True),
            # Owners can add experiments to any Collection they own
            PermissionTest("Collection", "private", "owner", Action.ADD_EXPERIMENT, True),
            PermissionTest("Collection", "published", "owner", Action.ADD_EXPERIMENT, True),
            # Collection admins can add experiments to any Collection they have admin role for
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_EXPERIMENT, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                collection_role="collection_admin",
            ),
            # Collection editors can add experiments to any Collection they have editor role for
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_EXPERIMENT, True, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                collection_role="collection_editor",
            ),
            # Collection viewers cannot add experiments to Collections
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_EXPERIMENT,
                False,
                403,
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                False,
                403,
                collection_role="collection_viewer",
            ),
            # Other users cannot add experiments to Collections
            PermissionTest("Collection", "private", "other_user", Action.ADD_EXPERIMENT, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.ADD_EXPERIMENT, False, 403),
            # Anonymous users cannot add experiments to Collections
            PermissionTest("Collection", "private", "anonymous", Action.ADD_EXPERIMENT, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.ADD_EXPERIMENT, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_experiment_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_experiment_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_experiment_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionAddScoreSetActionHandler:
    """Test the _handle_add_score_set_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can add score sets to any Collection
            PermissionTest("Collection", "private", "admin", Action.ADD_SCORE_SET, True),
            PermissionTest("Collection", "published", "admin", Action.ADD_SCORE_SET, True),
            # Owners can add score sets to any Collection they own
            PermissionTest("Collection", "private", "owner", Action.ADD_SCORE_SET, True),
            PermissionTest("Collection", "published", "owner", Action.ADD_SCORE_SET, True),
            # Collection admins can add score sets to any Collection they have admin role for
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_SCORE_SET, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.ADD_SCORE_SET, True, collection_role="collection_admin"
            ),
            # Collection editors can add score sets to any Collection they have editor role for
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_SCORE_SET, True, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                collection_role="collection_editor",
            ),
            # Collection viewers cannot add score sets to Collections
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_SCORE_SET,
                False,
                403,
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                False,
                403,
                collection_role="collection_viewer",
            ),
            # Other users cannot add score sets to Collections
            PermissionTest("Collection", "private", "other_user", Action.ADD_SCORE_SET, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.ADD_SCORE_SET, False, 403),
            # Anonymous users cannot add score sets to Collections
            PermissionTest("Collection", "private", "anonymous", Action.ADD_SCORE_SET, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.ADD_SCORE_SET, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_score_set_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_score_set_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_score_set_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionAddRoleActionHandler:
    """Test the _handle_add_role_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can add roles to any Collection
            PermissionTest("Collection", "private", "admin", Action.ADD_ROLE, True),
            PermissionTest("Collection", "published", "admin", Action.ADD_ROLE, True),
            # Owners can add roles to any Collection they own
            PermissionTest("Collection", "private", "owner", Action.ADD_ROLE, True),
            PermissionTest("Collection", "published", "owner", Action.ADD_ROLE, True),
            # Collection admins can add roles to any Collection they have admin role for
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_ROLE, True, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection", "published", "contributor", Action.ADD_ROLE, True, collection_role="collection_admin"
            ),
            # Collection editors cannot add roles to Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_ROLE, False, 403, collection_role="collection_editor"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                collection_role="collection_editor",
            ),
            # Collection viewers cannot add roles to Collections
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_ROLE, False, 403, collection_role="collection_viewer"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                collection_role="collection_viewer",
            ),
            # Other users cannot add roles to Collections
            PermissionTest("Collection", "private", "other_user", Action.ADD_ROLE, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.ADD_ROLE, False, 403),
            # Anonymous users cannot add roles to Collections
            PermissionTest("Collection", "private", "anonymous", Action.ADD_ROLE, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.ADD_ROLE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_role_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_role_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_role_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code


class TestCollectionAddBadgeActionHandler:
    """Test the _handle_add_badge_action helper function directly."""

    @pytest.mark.parametrize(
        "test_case",
        [
            # System admins can add badges to any Collection
            PermissionTest("Collection", "private", "admin", Action.ADD_BADGE, True),
            PermissionTest("Collection", "published", "admin", Action.ADD_BADGE, True),
            # Owners cannot add badges to Collections (admin-only operation)
            PermissionTest("Collection", "private", "owner", Action.ADD_BADGE, False, 403),
            PermissionTest("Collection", "published", "owner", Action.ADD_BADGE, False, 403),
            # Collection admins cannot add badges to Collections (system admin-only)
            PermissionTest(
                "Collection", "private", "contributor", Action.ADD_BADGE, False, 403, collection_role="collection_admin"
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                collection_role="collection_admin",
            ),
            # Collection editors cannot add badges to Collections
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                collection_role="collection_editor",
            ),
            # Collection viewers cannot add badges to Collections
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                collection_role="collection_viewer",
            ),
            # Other users cannot add badges to Collections
            PermissionTest("Collection", "private", "other_user", Action.ADD_BADGE, False, 404),
            PermissionTest("Collection", "published", "other_user", Action.ADD_BADGE, False, 403),
            # Anonymous users cannot add badges to Collections
            PermissionTest("Collection", "private", "anonymous", Action.ADD_BADGE, False, 404),
            PermissionTest("Collection", "published", "anonymous", Action.ADD_BADGE, False, 401),
        ],
        ids=lambda tc: f"{tc.user_type}_{tc.collection_role if tc.collection_role else 'no_role'}_{tc.entity_state}_{tc.action.value}_{'permitted' if tc.should_be_permitted else 'denied'}",
    )
    def test_handle_add_badge_action(self, test_case: PermissionTest, entity_helper: EntityTestHelper) -> None:
        """Test _handle_add_badge_action helper function directly."""
        assert test_case.entity_state is not None, "Collection tests must have entity_state"
        collection = entity_helper.create_collection(test_case.entity_state, collection_role=test_case.collection_role)
        user_data = entity_helper.create_user_data(test_case.user_type)

        private = test_case.entity_state == "private"
        official_collection = collection.badge_name is not None
        user_is_owner = test_case.user_type == "owner"
        collection_roles = [COLLECTION_ROLE_MAP[test_case.collection_role]] if test_case.collection_role else []
        active_roles = user_data.active_roles if user_data else []

        result = _handle_add_badge_action(
            user_data, collection, private, official_collection, user_is_owner, collection_roles, active_roles
        )

        assert result.permitted == test_case.should_be_permitted
        if not test_case.should_be_permitted and test_case.expected_code:
            assert result.http_code == test_case.expected_code
