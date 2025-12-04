"""Shared fixtures and helpers for permissions tests."""

from dataclasses import dataclass
from typing import Optional, Union
from unittest.mock import Mock

import pytest

from mavedb.lib.permissions.actions import Action
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole


@dataclass
class PermissionTest:
    """Represents a single permission test case for action handler testing.

    Used for parametrized testing of individual action handlers (_handle_read_action, etc.)
    rather than comprehensive end-to-end permission testing.

    Args:
        entity_type: Entity type name for context (not used in handler tests)
        entity_state: "private" or "published" (None for stateless entities like User)
        user_type: "admin", "owner", "contributor", "other_user", "anonymous", "self"
        action: Action enum value (for documentation, handlers test specific actions)
        should_be_permitted: True/False for normal cases, "NotImplementedError" for unsupported
        expected_code: HTTP error code when denied (403, 404, 401, etc.)
        description: Human-readable test description
        collection_role: For Collection tests: "collection_admin", "collection_editor", "collection_viewer"
        investigator_provided: For ScoreCalibration tests: True=investigator, False=community
    """

    entity_type: str
    entity_state: Optional[str]
    user_type: str
    action: Action
    should_be_permitted: Union[bool, str]
    expected_code: Optional[int] = None
    description: Optional[str] = None
    collection_role: Optional[str] = None
    collection_badge: Optional[str] = None
    investigator_provided: Optional[bool] = None


class EntityTestHelper:
    """Helper class to create test entities and user data with consistent properties."""

    @staticmethod
    def create_user_data(user_type: str):
        """Create UserData mock for different user types.

        Args:
            user_type: "admin", "owner", "contributor", "other_user", "anonymous", "self", "mapper"

        Returns:
            Mock UserData object or None for anonymous users
        """
        user_configs = {
            "admin": (1, "1111-1111-1111-111X", [UserRole.admin]),
            "owner": (2, "2222-2222-2222-222X", []),
            "contributor": (3, "3333-3333-3333-333X", []),
            "other_user": (4, "4444-4444-4444-444X", []),
            "self": (5, "5555-5555-5555-555X", []),
            "mapper": (6, "6666-6666-6666-666X", [UserRole.mapper]),
        }

        if user_type == "anonymous":
            return None

        if user_type not in user_configs:
            raise ValueError(f"Unknown user type: {user_type}")

        user_id, username, roles = user_configs[user_type]
        return Mock(user=Mock(id=user_id, username=username), active_roles=roles)

    @staticmethod
    def create_score_set(entity_state: str = "private", owner_id: int = 2):
        """Create a ScoreSet mock for testing."""
        private = entity_state == "private"
        published_date = None if private else "2023-01-01"
        contributors = [Mock(orcid_id="3333-3333-3333-333X")]

        return Mock(
            id=1,
            urn="urn:mavedb:00000001-a-1",
            private=private,
            created_by_id=owner_id,
            published_date=published_date,
            contributors=contributors,
        )

    @staticmethod
    def create_experiment(entity_state: str = "private", owner_id: int = 2):
        """Create an Experiment mock for testing."""
        private = entity_state == "private"
        published_date = None if private else "2023-01-01"
        contributors = [Mock(orcid_id="3333-3333-3333-333X")]

        return Mock(
            id=1,
            urn="urn:mavedb:00000001-a",
            private=private,
            created_by_id=owner_id,
            published_date=published_date,
            contributors=contributors,
        )

    @staticmethod
    def create_experiment_set(entity_state: str = "private", owner_id: int = 2):
        """Create an ExperimentSet mock for testing."""
        private = entity_state == "private"
        published_date = None if private else "2023-01-01"
        contributors = [Mock(orcid_id="3333-3333-3333-333X")]

        return Mock(
            id=1,
            urn="urn:mavedb:00000001",
            private=private,
            created_by_id=owner_id,
            published_date=published_date,
            contributors=contributors,
        )

    @staticmethod
    def create_collection(
        entity_state: str = "private",
        owner_id: int = 2,
        collection_role: Optional[str] = None,
        badge_name: Optional[str] = None,
    ):
        """Create a Collection mock for testing.

        Args:
            entity_state: "private" or "published"
            owner_id: ID of the collection owner
            collection_role: "collection_admin", "collection_editor", or "collection_viewer"
                           to create user association for contributor user (ID=3)
        """
        private = entity_state == "private"
        published_date = None if private else "2023-01-01"

        user_associations = []
        if collection_role:
            role_map = {
                "collection_admin": ContributionRole.admin,
                "collection_editor": ContributionRole.editor,
                "collection_viewer": ContributionRole.viewer,
            }
            user_associations.append(Mock(user_id=3, contribution_role=role_map[collection_role]))

        return Mock(
            id=1,
            urn="urn:mavedb:collection-001",
            private=private,
            created_by_id=owner_id,
            published_date=published_date,
            user_associations=user_associations,
            badge_name=badge_name,
        )

    @staticmethod
    def create_user(user_id: int = 5):
        """Create a User mock for testing."""
        return Mock(
            id=user_id,
            username=f"{user_id}{user_id}{user_id}{user_id}-{user_id}{user_id}{user_id}{user_id}-{user_id}{user_id}{user_id}{user_id}-{user_id}{user_id}{user_id}X",
        )

    @staticmethod
    def create_score_calibration(entity_state: str = "private", investigator_provided: bool = False):
        """Create a ScoreCalibration mock for testing.

        Args:
            entity_state: "private" or "published" (affects score_set and private property)
            investigator_provided: True if investigator-provided, False if community-provided
        """
        private = entity_state == "private"
        score_set = EntityTestHelper.create_score_set(entity_state)

        # ScoreCalibrations have their own private property plus associated ScoreSet
        return Mock(
            id=1,
            private=private,
            score_set=score_set,
            investigator_provided=investigator_provided,
            created_by_id=2,  # owner
            modified_by_id=2,  # owner
        )


@pytest.fixture
def entity_helper():
    """Fixture providing EntityTestHelper instance."""
    return EntityTestHelper()
