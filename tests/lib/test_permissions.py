from dataclasses import dataclass
from typing import Optional, Union
from unittest.mock import Mock

import pytest

from mavedb.lib.permissions import (
    Action,
    PermissionException,
    PermissionResponse,
    assert_permission,
    has_permission,
    roles_permitted,
)
from mavedb.models.collection import Collection
from mavedb.models.collection_user_association import CollectionUserAssociation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User


@dataclass
class PermissionTest:
    """Represents a single permission test case.

    Field Values:
    - entity_type: "ScoreSet", "Experiment", "ExperimentSet", "Collection", "User", "ScoreCalibration"
    - entity_state: "private", "published", or None (for entities like User without states)
    - user_type: "admin", "owner", "contributor", "other_user", "anonymous", "self"
      - For Collections: "contributor" is generic, use collection_role to specify "collection_admin", "collection_editor", "collection_viewer"
    - action: Action enum value (READ, UPDATE, DELETE, ADD_SCORE_SET, etc.)
    - should_be_permitted: True if permission should be granted, False if denied, "NotImplementedError" if action not supported
    - expected_code: HTTP error code when permission denied (403, 404, 401, etc.)
    - description: Human-readable test description
    - investigator_provided: True/False for ScoreCalibration tests, None for other entities
    - collection_role: "collection_admin", "collection_editor", "collection_viewer" for Collection entity tests, None for others
    """

    entity_type: str  # "ScoreSet", "Experiment", "ExperimentSet", "Collection", "User", "ScoreCalibration"
    entity_state: Optional[str]  # "private", "published", or None for stateless entities
    user_type: str  # "admin", "owner", "contributor", "other_user", "anonymous", "self"
    action: Action
    should_be_permitted: Union[bool, str]  # True/False for normal cases, "NotImplementedError" for unsupported actions
    expected_code: Optional[int] = None  # HTTP error code when denied (403, 404, 401)
    description: Optional[str] = None
    investigator_provided: Optional[bool] = None  # ScoreCalibration: True=investigator, False=community
    collection_role: Optional[str] = (
        None  # "collection_admin", "collection_editor", "collection_viewer" for Collection tests
    )


class PermissionTestData:
    """Contains all explicit permission test cases."""

    @staticmethod
    def get_all_permission_tests() -> list[PermissionTest]:
        """Get all permission test cases in one explicit list."""
        return [
            # =============================================================================
            # EXPERIMENT SET PERMISSIONS
            # =============================================================================
            # ExperimentSet READ permissions - Private
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private experiment set",
            ),
            # ExperimentSet READ permissions - Published
            PermissionTest(
                "ExperimentSet",
                "published",
                "admin",
                Action.READ,
                True,
                description="Admin can read published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "owner",
                Action.READ,
                True,
                description="Owner can read their published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "other_user",
                Action.READ,
                True,
                description="Other user can read published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "anonymous",
                Action.READ,
                True,
                description="Anonymous can read published experiment set",
            ),
            # ExperimentSet UPDATE permissions - Private
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private experiment set update",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private experiment set update",
            ),
            # ExperimentSet UPDATE permissions - Published
            PermissionTest(
                "ExperimentSet",
                "published",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "other_user",
                Action.UPDATE,
                False,
                403,
                "Other user cannot update published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "anonymous",
                Action.UPDATE,
                False,
                401,
                "Anonymous cannot update published experiment set",
            ),
            # ExperimentSet DELETE permissions - Private (unpublished)
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "contributor",
                Action.DELETE,
                True,
                description="Contributor can delete unpublished experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private experiment set delete",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private experiment set delete",
            ),
            # ExperimentSet DELETE permissions - Published
            PermissionTest(
                "ExperimentSet",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "owner",
                Action.DELETE,
                False,
                403,
                "Owner cannot delete published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Contributor cannot delete published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user cannot delete published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                403,
                "Anonymous cannot delete published experiment set",
            ),
            # ExperimentSet ADD_EXPERIMENT permissions - Private
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.ADD_EXPERIMENT,
                True,
                description="Admin can add experiment to private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "owner",
                Action.ADD_EXPERIMENT,
                True,
                description="Owner can add experiment to their experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Contributor can add experiment to experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "other_user",
                Action.ADD_EXPERIMENT,
                False,
                404,
                "Other user gets 404 for private experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "anonymous",
                Action.ADD_EXPERIMENT,
                False,
                404,
                "Anonymous gets 404 for private experiment set",
            ),
            # ExperimentSet ADD_EXPERIMENT permissions - Published
            PermissionTest(
                "ExperimentSet",
                "published",
                "admin",
                Action.ADD_EXPERIMENT,
                True,
                description="Admin can add experiment to published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "owner",
                Action.ADD_EXPERIMENT,
                True,
                description="Owner can add experiment to their published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Contributor can add experiment to published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "other_user",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Other user cannot add experiment to published experiment set",
            ),
            PermissionTest(
                "ExperimentSet",
                "published",
                "anonymous",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Anonymous cannot add experiment to experiment set",
            ),
            # =============================================================================
            # EXPERIMENT PERMISSIONS
            # =============================================================================
            # Experiment READ permissions - Private
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private experiment",
            ),
            # Experiment READ permissions - Published
            PermissionTest(
                "Experiment",
                "published",
                "admin",
                Action.READ,
                True,
                description="Admin can read published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "owner",
                Action.READ,
                True,
                description="Owner can read their published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "other_user",
                Action.READ,
                True,
                description="Other user can read published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "anonymous",
                Action.READ,
                True,
                description="Anonymous can read published experiment",
            ),
            # Experiment UPDATE permissions - Private
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private experiment update",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private experiment update",
            ),
            # Experiment UPDATE permissions - Published
            PermissionTest(
                "Experiment",
                "published",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "other_user",
                Action.UPDATE,
                False,
                403,
                "Other user cannot update published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "anonymous",
                Action.UPDATE,
                False,
                401,
                "Anonymous cannot update published experiment",
            ),
            # Experiment DELETE permissions - Private (unpublished)
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "contributor",
                Action.DELETE,
                True,
                description="Contributor can delete unpublished experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private experiment delete",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private experiment delete",
            ),
            # Experiment DELETE permissions - Published
            PermissionTest(
                "Experiment",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "owner",
                Action.DELETE,
                False,
                403,
                "Owner cannot delete published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Contributor cannot delete published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user gets 403 for published experiment delete",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                403,
                "Anonymous gets 403 for published experiment delete",
            ),
            # Experiment ADD_SCORE_SET permissions - Private
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.ADD_SCORE_SET,
                True,
                description="Admin can add score set to private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "owner",
                Action.ADD_SCORE_SET,
                True,
                description="Owner can add score set to their private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Contributor can add score set to private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "other_user",
                Action.ADD_SCORE_SET,
                False,
                404,
                "Other user gets 404 for private experiment",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "anonymous",
                Action.ADD_SCORE_SET,
                False,
                404,
                "Anonymous gets 404 for private experiment",
            ),
            # Experiment ADD_SCORE_SET permissions - Published (any signed in user can add)
            PermissionTest(
                "Experiment",
                "published",
                "admin",
                Action.ADD_SCORE_SET,
                True,
                description="Admin can add score set to published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "owner",
                Action.ADD_SCORE_SET,
                True,
                description="Owner can add score set to their published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Contributor can add score set to published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "other_user",
                Action.ADD_SCORE_SET,
                True,
                description="Other user can add score set to published experiment",
            ),
            PermissionTest(
                "Experiment",
                "published",
                "anonymous",
                Action.ADD_SCORE_SET,
                False,
                403,
                "Anonymous cannot add score set to experiment",
            ),
            # =============================================================================
            # SCORE SET PERMISSIONS
            # =============================================================================
            # ScoreSet READ permissions - Private
            PermissionTest(
                "ScoreSet",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private score set",
            ),
            # ScoreSet READ permissions - Published
            PermissionTest(
                "ScoreSet",
                "published",
                "admin",
                Action.READ,
                True,
                description="Admin can read published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "owner",
                Action.READ,
                True,
                description="Owner can read their published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "other_user",
                Action.READ,
                True,
                description="Other user can read published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "anonymous",
                Action.READ,
                True,
                description="Anonymous can read published score set",
            ),
            # ScoreSet UPDATE permissions - Private
            PermissionTest(
                "ScoreSet",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private score set update",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private score set update",
            ),
            # ScoreSet UPDATE permissions - Published
            PermissionTest(
                "ScoreSet",
                "published",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "other_user",
                Action.UPDATE,
                False,
                403,
                "Other user cannot update published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "anonymous",
                Action.UPDATE,
                False,
                401,
                "Anonymous cannot update published score set",
            ),
            # ScoreSet DELETE permissions - Private (unpublished)
            PermissionTest(
                "ScoreSet",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "contributor",
                Action.DELETE,
                True,
                description="Contributor can delete unpublished score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private score set delete",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private score set delete",
            ),
            # ScoreSet DELETE permissions - Published
            PermissionTest(
                "ScoreSet",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "owner",
                Action.DELETE,
                False,
                403,
                "Owner cannot delete published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Contributor cannot delete published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user gets 403 for published score set delete",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                403,
                "Anonymous gets 403 for published score set delete",
            ),
            # ScoreSet PUBLISH permissions - Private
            PermissionTest(
                "ScoreSet",
                "private",
                "admin",
                Action.PUBLISH,
                True,
                description="Admin can publish private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "owner",
                Action.PUBLISH,
                True,
                description="Owner can publish their private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "contributor",
                Action.PUBLISH,
                True,
                description="Contributor can publish private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "other_user",
                Action.PUBLISH,
                False,
                404,
                "Other user gets 404 for private score set publish",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "anonymous",
                Action.PUBLISH,
                False,
                404,
                "Anonymous gets 404 for private score set publish",
            ),
            # ScoreSet SET_SCORES permissions - Private
            PermissionTest(
                "ScoreSet",
                "private",
                "admin",
                Action.SET_SCORES,
                True,
                description="Admin can set scores on private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "owner",
                Action.SET_SCORES,
                True,
                description="Owner can set scores on their private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "contributor",
                Action.SET_SCORES,
                True,
                description="Contributor can set scores on private score set",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "other_user",
                Action.SET_SCORES,
                False,
                404,
                "Other user gets 404 for private score set scores",
            ),
            PermissionTest(
                "ScoreSet",
                "private",
                "anonymous",
                Action.SET_SCORES,
                False,
                404,
                "Anonymous gets 404 for private score set scores",
            ),
            # ScoreSet SET_SCORES permissions - Published
            PermissionTest(
                "ScoreSet",
                "published",
                "admin",
                Action.SET_SCORES,
                True,
                description="Admin can set scores on published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "owner",
                Action.SET_SCORES,
                True,
                description="Owner can set scores on their published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "contributor",
                Action.SET_SCORES,
                True,
                description="Contributor can set scores on published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "other_user",
                Action.SET_SCORES,
                False,
                403,
                "Other user cannot set scores on published score set",
            ),
            PermissionTest(
                "ScoreSet",
                "published",
                "anonymous",
                Action.SET_SCORES,
                False,
                403,
                "Anonymous cannot set scores on score set",
            ),
            # =============================================================================
            # COLLECTION PERMISSIONS
            # =============================================================================
            # Collection READ permissions - Private
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Collection admin can read private collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Collection editor can read private collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Collection viewer can read private collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private collection",
            ),
            # Collection READ permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.READ,
                True,
                description="Admin can read published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.READ,
                True,
                description="Owner can read their published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Collection admin can read published collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Collection editor can read published collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.READ,
                True,
                description="Collection viewer can read published collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.READ,
                True,
                description="Other user can read published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.READ,
                True,
                description="Anonymous can read published collection",
            ),
            # Collection UPDATE permissions - Private
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Collection admin can update private collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Collection editor can update private collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.UPDATE,
                False,
                403,
                "Collection viewer cannot update private collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private collection update",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private collection update",
            ),
            # Collection UPDATE permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.UPDATE,
                True,
                description="Collection admin can update published collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.UPDATE,
                True,
                description="Collection editor can update published collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.UPDATE,
                False,
                403,
                "Collection viewer cannot update published collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.UPDATE,
                False,
                403,
                "Other user cannot update published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.UPDATE,
                False,
                401,
                "Anonymous cannot update published collection",
            ),
            # Collection DELETE permissions - Private
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection admin cannot delete private collection (only owner can)",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection editor cannot delete private collection (only owner can)",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection viewer cannot delete private collection (only owner can)",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private collection delete",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private collection delete",
            ),
            # Collection DELETE permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published collection",
            ),
            # TODO: only admins can delete collections with badges
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete published collection w/o badges",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection admin cannot delete published collection (only MaveDB admin can)",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection editor cannot delete published collection (only MaveDB admin can)",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Collection viewer cannot delete published collection (only MaveDB admin can)",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user cannot delete published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                403,
                "Anonymous cannot delete published collection",
            ),
            # Collection PUBLISH permissions - Private
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.PUBLISH,
                True,
                description="MaveDB admin can publish private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.PUBLISH,
                True,
                description="Owner can publish their collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.PUBLISH,
                True,
                description="Collection admin can publish collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.PUBLISH,
                False,
                403,
                "Collection editor cannot publish collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.PUBLISH,
                False,
                403,
                "Collection viewer cannot publish collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.PUBLISH,
                False,
                404,
                "Other user gets 404 for private collection publish",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.PUBLISH,
                False,
                404,
                "Anonymous gets 404 for private collection publish",
            ),
            # Collection ADD_EXPERIMENT permissions - Private (Collections add experiments, not experiment sets)
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.ADD_EXPERIMENT,
                True,
                description="Admin can add experiment to private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.ADD_EXPERIMENT,
                True,
                description="Owner can add experiment to their collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Collection admin can add experiment to collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Collection editor can add experiment to collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Collection viewer cannot add experiment to private collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.ADD_EXPERIMENT,
                False,
                404,
                "Other user gets 404 for private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.ADD_EXPERIMENT,
                False,
                404,
                "Anonymous gets 404 for private collection",
            ),
            # Collection ADD_EXPERIMENT permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.ADD_EXPERIMENT,
                True,
                description="Admin can add experiment to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.ADD_EXPERIMENT,
                True,
                description="Owner can add experiment to their published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Collection admin can add experiment to published collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                True,
                description="Collection editor can add experiment to published collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Collection viewer cannot add experiment to published collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Other user cannot add to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.ADD_EXPERIMENT,
                False,
                403,
                "Anonymous cannot add to collection",
            ),
            # Collection ADD_SCORE_SET permissions - Private
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.ADD_SCORE_SET,
                True,
                description="Admin can add score set to private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.ADD_SCORE_SET,
                True,
                description="Owner can add score set to their private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Collection admin can add score set to private collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Collection editor can add score set to private collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_SCORE_SET,
                False,
                403,
                "Collection viewer cannot add score set to private collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.ADD_SCORE_SET,
                False,
                404,
                "Other user gets 404 for private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.ADD_SCORE_SET,
                False,
                404,
                "Anonymous gets 404 for private collection",
            ),
            # Collection ADD_SCORE_SET permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.ADD_SCORE_SET,
                True,
                description="Admin can add score set to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.ADD_SCORE_SET,
                True,
                description="Owner can add score set to their published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Collection admin can add score set to published collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                True,
                description="Collection editor can add score set to published collection",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_SCORE_SET,
                False,
                403,
                "Collection viewer cannot add score set to published collection",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.ADD_SCORE_SET,
                False,
                403,
                "Other user cannot add score set to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.ADD_SCORE_SET,
                False,
                403,
                "Anonymous cannot add score set to collection",
            ),
            # Collection ADD_ROLE permissions
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.ADD_ROLE,
                True,
                description="Admin can add roles to private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "owner",
                Action.ADD_ROLE,
                True,
                description="Owner can add roles to their collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_ROLE,
                True,
                description="Collection admin can add roles to collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                "Collection editor cannot add roles to collection (only admin can)",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "private",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                "Collection viewer cannot add roles to collection (only admin can)",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "private",
                "other_user",
                Action.ADD_ROLE,
                False,
                404,
                "Other user gets 404 for private collection",
            ),
            PermissionTest(
                "Collection",
                "private",
                "anonymous",
                Action.ADD_ROLE,
                False,
                404,
                "Anonymous gets 404 for private collection",
            ),
            # Collection ADD_ROLE permissions - Published
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.ADD_ROLE,
                True,
                description="Admin can add roles to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.ADD_ROLE,
                True,
                description="Owner can add roles to their published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_ROLE,
                True,
                description="Collection admin can add roles to published collection",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                "Collection editor cannot add roles to published collection (only admin can)",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_ROLE,
                False,
                403,
                "Collection viewer cannot add roles to published collection (only admin can)",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.ADD_ROLE,
                False,
                403,
                "Other user cannot add roles to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.ADD_ROLE,
                False,
                403,
                "Anonymous cannot add roles to published collection",
            ),
            # Collection ADD_BADGE permissions (only admin)
            PermissionTest(
                "Collection",
                "published",
                "admin",
                Action.ADD_BADGE,
                True,
                description="Admin can add badge to published collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "owner",
                Action.ADD_BADGE,
                False,
                403,
                "Owner cannot add badge to collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                "Collection admin cannot add badge to collection (only MaveDB admin can)",
                collection_role="collection_admin",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                "Collection editor cannot add badge to collection (only MaveDB admin can)",
                collection_role="collection_editor",
            ),
            PermissionTest(
                "Collection",
                "published",
                "contributor",
                Action.ADD_BADGE,
                False,
                403,
                "Collection viewer cannot add badge to collection (only MaveDB admin can)",
                collection_role="collection_viewer",
            ),
            PermissionTest(
                "Collection",
                "published",
                "other_user",
                Action.ADD_BADGE,
                False,
                403,
                "Other user cannot add badge to collection",
            ),
            PermissionTest(
                "Collection",
                "published",
                "anonymous",
                Action.ADD_BADGE,
                False,
                401,
                "Anonymous cannot add badge to collection",
            ),
            # =============================================================================
            # USER PERMISSIONS
            # =============================================================================
            # User READ permissions (accessing user profiles)
            PermissionTest(
                "User",
                None,
                "admin",
                Action.READ,
                True,
                description="Admin can read any user profile",
            ),
            PermissionTest(
                "User",
                None,
                "self",
                Action.READ,
                True,
                description="User can read their own profile",
            ),
            PermissionTest(
                "User",
                None,
                "other_user",
                Action.READ,
                False,
                403,
                description="Users cannot read other user profiles",
            ),
            PermissionTest(
                "User",
                None,
                "anonymous",
                Action.READ,
                False,
                401,
                description="Anonymous cannot read user profiles",
            ),
            # User UPDATE permissions
            PermissionTest(
                "User",
                None,
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update any user profile",
            ),
            PermissionTest(
                "User",
                None,
                "self",
                Action.UPDATE,
                True,
                description="User can update their own profile",
            ),
            PermissionTest(
                "User",
                None,
                "other_user",
                Action.UPDATE,
                False,
                403,
                "User cannot update other user profiles",
            ),
            PermissionTest(
                "User",
                None,
                "anonymous",
                Action.UPDATE,
                False,
                401,
                "Anonymous cannot update user profiles",
            ),
            # User DELETE permissions - not implemented
            # User LOOKUP permissions (for search/autocomplete)
            PermissionTest(
                "User",
                None,
                "admin",
                Action.LOOKUP,
                True,
                description="Admin can lookup users",
            ),
            PermissionTest(
                "User",
                None,
                "owner",
                Action.LOOKUP,
                True,
                description="User can lookup other users",
            ),
            PermissionTest(
                "User",
                None,
                "contributor",
                Action.LOOKUP,
                True,
                description="User can lookup other users",
            ),
            PermissionTest(
                "User",
                None,
                "other_user",
                Action.LOOKUP,
                True,
                description="User can lookup other users",
            ),
            PermissionTest(
                "User",
                None,
                "anonymous",
                Action.LOOKUP,
                False,
                401,
                "Anonymous cannot lookup users",
            ),
            # User ADD_ROLE permissions
            PermissionTest(
                "User",
                None,
                "admin",
                Action.ADD_ROLE,
                True,
                description="Admin can add roles to users",
            ),
            PermissionTest(
                "User",
                None,
                "self",
                Action.ADD_ROLE,
                False,
                403,
                "User cannot add roles to themselves",
            ),
            PermissionTest(
                "User",
                None,
                "other_user",
                Action.ADD_ROLE,
                False,
                403,
                "User cannot add roles to others",
            ),
            PermissionTest(
                "User",
                None,
                "anonymous",
                Action.ADD_ROLE,
                False,
                401,
                "Anonymous cannot add roles",
            ),
            # =============================================================================
            # SCORE CALIBRATION PERMISSIONS
            # =============================================================================
            # ScoreCalibration READ permissions - Private, Investigator Provided
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.READ,
                True,
                description="Contributor can read private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private investigator calibration",
                investigator_provided=True,
            ),
            # ScoreCalibration UPDATE permissions - Private, Investigator Provided (follows score set model)
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.UPDATE,
                True,
                description="Contributor can update private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private investigator calibration update",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private investigator calibration update",
                investigator_provided=True,
            ),
            # ScoreCalibration READ permissions - Private, Community Provided
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.READ,
                True,
                description="Admin can read private community calibration",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.READ,
                True,
                description="Owner can read their private community calibration",
                investigator_provided=False,
            ),
            # NOTE: Contributors do not exist for community-provided calibrations
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.READ,
                False,
                404,
                "Other user gets 404 for private community calibration",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.READ,
                False,
                404,
                "Anonymous gets 404 for private community calibration",
                investigator_provided=False,
            ),
            # ScoreCalibration UPDATE permissions - Private, Community Provided (only owner can edit)
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.UPDATE,
                True,
                description="Admin can update private community calibration",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.UPDATE,
                True,
                description="Owner can update their private community calibration",
                investigator_provided=False,
            ),
            # NOTE: Contributors do not exist for community-provided calibrations
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.UPDATE,
                False,
                404,
                "Other user gets 404 for private community calibration update",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.UPDATE,
                False,
                404,
                "Anonymous gets 404 for private community calibration update",
                investigator_provided=False,
            ),
            # ScoreCalibration PUBLISH permissions - Private
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.PUBLISH,
                True,
                description="Admin can publish private calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.PUBLISH,
                True,
                description="Owner can publish their private calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.PUBLISH,
                True,
                description="Contributor can publish private investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.PUBLISH,
                False,
                404,
                "Other user gets 404 for private calibration publish",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.PUBLISH,
                False,
                404,
                "Anonymous gets 404 for private calibration publish",
                investigator_provided=True,
            ),
            # ScoreCalibration CHANGE_RANK permissions - Private
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.CHANGE_RANK,
                True,
                description="Admin can change calibration rank",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.CHANGE_RANK,
                True,
                description="Owner can change their calibration rank",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.CHANGE_RANK,
                True,
                description="Contributor can change investigator calibration rank",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.CHANGE_RANK,
                False,
                404,
                "Other user gets 404 for private calibration rank change",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.CHANGE_RANK,
                False,
                404,
                "Anonymous gets 404 for private calibration rank change",
                investigator_provided=True,
            ),
            # ScoreCalibration DELETE permissions - Private (investigator-provided)
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "contributor",
                Action.DELETE,
                True,
                description="Contributor can delete unpublished investigator calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private calibration delete",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private calibration delete",
                investigator_provided=True,
            ),
            # ScoreCalibration DELETE permissions - Published (investigator-provided)
            PermissionTest(
                "ScoreCalibration",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "owner",
                Action.DELETE,
                False,
                403,
                "Owner cannot delete published calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "contributor",
                Action.DELETE,
                False,
                403,
                "Contributor cannot delete published calibration",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user gets 403 for published calibration delete",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                401,
                "Anonymous gets 401 for published calibration delete",
                investigator_provided=True,
            ),
            # ScoreCalibration DELETE permissions - Private (community-provided)
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete private community calibration",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "owner",
                Action.DELETE,
                True,
                description="Owner can delete unpublished community calibration",
                investigator_provided=False,
            ),
            # NOTE: Contributors do not exist for community-provided calibrations
            PermissionTest(
                "ScoreCalibration",
                "private",
                "other_user",
                Action.DELETE,
                False,
                404,
                "Other user gets 404 for private community calibration delete",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "anonymous",
                Action.DELETE,
                False,
                404,
                "Anonymous gets 404 for private community calibration delete",
                investigator_provided=False,
            ),
            # ScoreCalibration DELETE permissions - Published (community-provided)
            PermissionTest(
                "ScoreCalibration",
                "published",
                "admin",
                Action.DELETE,
                True,
                description="Admin can delete published community calibration",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "owner",
                Action.DELETE,
                False,
                403,
                "Owner cannot delete published community calibration",
                investigator_provided=False,
            ),
            # NOTE: Contributors do not exist for community-provided calibrations
            PermissionTest(
                "ScoreCalibration",
                "published",
                "other_user",
                Action.DELETE,
                False,
                403,
                "Other user gets 403 for published community calibration delete",
                investigator_provided=False,
            ),
            PermissionTest(
                "ScoreCalibration",
                "published",
                "anonymous",
                Action.DELETE,
                False,
                401,
                "Anonymous gets 401 for published community calibration delete",
                investigator_provided=False,
            ),
            # ===========================
            # NotImplementedError Test Cases
            # ===========================
            # These test cases expect NotImplementedError for unsupported action/entity combinations
            # ExperimentSet unsupported actions
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.PUBLISH,
                "NotImplementedError",
                description="ExperimentSet PUBLISH not implemented",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.SET_SCORES,
                "NotImplementedError",
                description="ExperimentSet SET_SCORES not implemented",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.ADD_ROLE,
                "NotImplementedError",
                description="ExperimentSet ADD_ROLE not implemented",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.ADD_BADGE,
                "NotImplementedError",
                description="ExperimentSet ADD_BADGE not implemented",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.CHANGE_RANK,
                "NotImplementedError",
                description="ExperimentSet CHANGE_RANK not implemented",
            ),
            PermissionTest(
                "ExperimentSet",
                "private",
                "admin",
                Action.LOOKUP,
                "NotImplementedError",
                description="ExperimentSet LOOKUP not implemented",
            ),
            # Experiment unsupported actions
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.PUBLISH,
                "NotImplementedError",
                description="Experiment PUBLISH not implemented",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.SET_SCORES,
                "NotImplementedError",
                description="Experiment SET_SCORES not implemented",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.ADD_ROLE,
                "NotImplementedError",
                description="Experiment ADD_ROLE not implemented",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.ADD_BADGE,
                "NotImplementedError",
                description="Experiment ADD_BADGE not implemented",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.CHANGE_RANK,
                "NotImplementedError",
                description="Experiment CHANGE_RANK not implemented",
            ),
            PermissionTest(
                "Experiment",
                "private",
                "admin",
                Action.LOOKUP,
                "NotImplementedError",
                description="Experiment LOOKUP not implemented",
            ),
            # Collection unsupported actions
            PermissionTest(
                "Collection",
                "private",
                "admin",
                Action.LOOKUP,
                "NotImplementedError",
                description="Collection LOOKUP not implemented",
            ),
            # ScoreCalibration unsupported actions
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.ADD_EXPERIMENT,
                "NotImplementedError",
                description="ScoreCalibration ADD_EXPERIMENT not implemented",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.ADD_SCORE_SET,
                "NotImplementedError",
                description="ScoreCalibration ADD_SCORE_SET not implemented",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.ADD_ROLE,
                "NotImplementedError",
                description="ScoreCalibration ADD_ROLE not implemented",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.ADD_BADGE,
                "NotImplementedError",
                description="ScoreCalibration ADD_BADGE not implemented",
                investigator_provided=True,
            ),
            PermissionTest(
                "ScoreCalibration",
                "private",
                "admin",
                Action.LOOKUP,
                "NotImplementedError",
                description="ScoreCalibration LOOKUP not implemented",
                investigator_provided=True,
            ),
            # User unsupported actions
            PermissionTest(
                "User",
                None,
                "admin",
                Action.DELETE,
                "NotImplementedError",
                description="User DELETE not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.PUBLISH,
                "NotImplementedError",
                description="User PUBLISH not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.SET_SCORES,
                "NotImplementedError",
                description="User SET_SCORES not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.ADD_SCORE_SET,
                "NotImplementedError",
                description="User ADD_SCORE_SET not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.ADD_EXPERIMENT,
                "NotImplementedError",
                description="User ADD_EXPERIMENT not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.ADD_BADGE,
                "NotImplementedError",
                description="User ADD_BADGE not implemented",
            ),
            PermissionTest(
                "User",
                None,
                "admin",
                Action.CHANGE_RANK,
                "NotImplementedError",
                description="User CHANGE_RANK not implemented",
            ),
        ]


class EntityTestHelper:
    """Helper methods for creating test entities with specific states."""

    @staticmethod
    def create_score_set(owner_id: int = 2, contributors: list = []) -> ScoreSet:
        """Create a private ScoreSet for testing."""
        score_set = Mock(spec=ScoreSet)
        score_set.urn = "urn:mavedb:00000001-a-1"
        score_set.created_by_id = owner_id
        score_set.contributors = [Mock(orcid_id=c) for c in contributors]
        return score_set

    @staticmethod
    def create_experiment(owner_id: int = 2, contributors: list = []) -> Experiment:
        """Create a private Experiment for testing."""
        experiment = Mock(spec=Experiment)
        experiment.urn = "urn:mavedb:00000001-a"
        experiment.created_by_id = owner_id
        experiment.contributors = [Mock(orcid_id=c) for c in contributors]
        return experiment

    @staticmethod
    def create_investigator_calibration(owner_id: int = 2, contributors: list = []):
        """Create an investigator-provided score calibration for testing."""
        calibration = Mock(spec=ScoreCalibration)
        calibration.id = 1
        calibration.created_by_id = owner_id
        calibration.investigator_provided = True
        calibration.score_set = EntityTestHelper.create_score_set(owner_id, contributors)
        return calibration

    @staticmethod
    def create_community_calibration(owner_id: int = 2, contributors: list = []):
        """Create a community-provided score calibration for testing."""
        calibration = Mock(spec=ScoreCalibration)
        calibration.id = 1
        calibration.created_by_id = owner_id
        calibration.investigator_provided = False
        calibration.score_set = EntityTestHelper.create_score_set(owner_id, contributors)
        return calibration

    @staticmethod
    def create_experiment_set(owner_id: int = 2, contributors: list = []) -> ExperimentSet:
        """Create an ExperimentSet for testing in the specified state."""
        exp_set = Mock(spec=ExperimentSet)
        exp_set.urn = "urn:mavedb:00000001"
        exp_set.created_by_id = owner_id
        exp_set.contributors = [Mock(orcid_id=c) for c in contributors]
        return exp_set

    @staticmethod
    def create_collection(
        owner_id: int = 2,
        user_role: Optional[str] = None,
        user_id: int = 3,
    ) -> Collection:
        """Create a Collection for testing."""
        collection = Mock(spec=Collection)
        collection.urn = "urn:mavedb:col000001"
        collection.created_by_id = owner_id
        collection.badge_name = None  # Not an official collection by default

        # Create user_associations for Collection permissions
        user_associations = []

        # Add owner as admin (unless owner_id is the same as user_id with a specific role)
        if not (user_role and user_id == owner_id):
            owner_assoc = Mock(spec=CollectionUserAssociation)
            owner_assoc.user_id = owner_id
            owner_assoc.contribution_role = ContributionRole.admin
            user_associations.append(owner_assoc)

        # Add specific role if requested
        if user_role:
            role_mapping = {
                "collection_admin": ContributionRole.admin,
                "collection_editor": ContributionRole.editor,
                "collection_viewer": ContributionRole.viewer,
            }
            if user_role in role_mapping:
                user_assoc = Mock(spec=CollectionUserAssociation)
                user_assoc.user_id = user_id
                user_assoc.contribution_role = role_mapping[user_role]
                user_associations.append(user_assoc)

        collection.user_associations = user_associations
        return collection

    @staticmethod
    def create_user(user_id: int = 3) -> User:
        """Create a User for testing."""
        user = Mock(spec=User)
        user.id = user_id
        user.username = "3333-3333-3333-333X"
        user.email = "target@example.com"
        user.first_name = "Target"
        user.last_name = "User"
        user.is_active = True
        return user


class TestPermissions:
    """Test all permission scenarios."""

    def setup_method(self):
        """Set up test data for each test method."""
        self.users = {
            "admin": Mock(user=Mock(id=1, username="1111-1111-1111-111X"), active_roles=[UserRole.admin]),
            "owner": Mock(user=Mock(id=2, username="2222-2222-2222-222X"), active_roles=[]),
            "contributor": Mock(user=Mock(id=3, username="3333-3333-3333-333X"), active_roles=[]),
            "other_user": Mock(user=Mock(id=4, username="4444-4444-4444-444X"), active_roles=[]),
            "self": Mock(  # For User entity tests where user is checking themselves
                user=Mock(id=3, username="3333-3333-3333-333X"), active_roles=[]
            ),
            "anonymous": None,
        }

    @pytest.mark.parametrize(
        "test_case",
        PermissionTestData.get_all_permission_tests(),
        ids=lambda tc: f"{tc.entity_type}_{tc.entity_state or 'no_state'}_{tc.user_type}_{tc.action.value}",
    )
    def test_permission(self, test_case: PermissionTest):
        """Test a single permission scenario."""
        # Handle NotImplementedError test cases
        if test_case.should_be_permitted == "NotImplementedError":
            with pytest.raises(NotImplementedError):
                entity = self._create_entity(test_case)
                user_data = self.users[test_case.user_type]
                has_permission(user_data, entity, test_case.action)
            return

        # Arrange - Create entity for normal test cases
        entity = self._create_entity(test_case)
        user_data = self.users[test_case.user_type]

        # Act
        result = has_permission(user_data, entity, test_case.action)

        # Assert
        assert result.permitted == test_case.should_be_permitted, (
            f"Expected {test_case.should_be_permitted} but got {result.permitted}. "
            f"Description: {test_case.description}"
        )

        if not test_case.should_be_permitted and test_case.expected_code:
            assert (
                result.http_code == test_case.expected_code
            ), f"Expected error code {test_case.expected_code} but got {result.http_code}"

    def _create_entity(self, test_case: PermissionTest):
        """Create an entity based on the test case specification."""
        # For most entities, contributors are ORCiDs. For Collections, handle roles differently.
        if test_case.entity_type == "Collection":
            # Collection uses specific role-based permissions
            contributors = []  # Don't use ORCiD contributors for Collections
        else:
            contributors = ["3333-3333-3333-333X"] if test_case.user_type == "contributor" else []

        if test_case.entity_type == "ScoreSet":
            entity = EntityTestHelper.create_score_set(owner_id=2, contributors=contributors)
            entity.private = test_case.entity_state == "private"
            entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
            return entity

        elif test_case.entity_type == "Experiment":
            entity = EntityTestHelper.create_experiment(owner_id=2, contributors=contributors)
            entity.private = test_case.entity_state == "private"
            entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
            return entity

        elif test_case.entity_type == "ScoreCalibration":
            if test_case.investigator_provided is True:
                entity = EntityTestHelper.create_investigator_calibration(owner_id=2, contributors=contributors)
                entity.private = test_case.entity_state == "private"
                entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
                return entity
            elif test_case.investigator_provided is False:
                entity = EntityTestHelper.create_community_calibration(owner_id=2, contributors=contributors)
                entity.private = test_case.entity_state == "private"
                entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
                return entity

        elif test_case.entity_type == "ExperimentSet":
            entity = EntityTestHelper.create_experiment_set(owner_id=2, contributors=contributors)
            entity.private = test_case.entity_state == "private"
            entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
            return entity

        elif test_case.entity_type == "Collection":
            entity = EntityTestHelper.create_collection(
                owner_id=2,
                user_role=test_case.collection_role,
                user_id=3,  # User ID for the collection contributor role
            )
            entity.private = test_case.entity_state == "private"
            entity.published_date = "2023-01-01" if test_case.entity_state == "published" else None
            return entity

        elif test_case.entity_type == "User":
            # For User tests, create a target user (id=3) that will be acted upon
            return EntityTestHelper.create_user(user_id=3)

        raise ValueError(f"Unknown entity type/state: {test_case.entity_type}/{test_case.entity_state}")


class TestPermissionResponse:
    """Test PermissionResponse class functionality."""

    def test_permission_response_permitted(self):
        """Test PermissionResponse when permission is granted."""
        response = PermissionResponse(True)

        assert response.permitted is True
        assert response.http_code is None
        assert response.message is None

    def test_permission_response_denied_default_code(self):
        """Test PermissionResponse when permission is denied with default error code."""
        response = PermissionResponse(False)

        assert response.permitted is False
        assert response.http_code == 403
        assert response.message is None

    def test_permission_response_denied_custom_code_and_message(self):
        """Test PermissionResponse when permission is denied with custom error code and message."""
        response = PermissionResponse(False, 404, "Resource not found")

        assert response.permitted is False
        assert response.http_code == 404
        assert response.message == "Resource not found"


class TestPermissionException:
    """Test PermissionException class functionality."""

    def test_permission_exception_creation(self):
        """Test PermissionException creation and properties."""
        exception = PermissionException(403, "Insufficient permissions")

        assert exception.http_code == 403
        assert exception.message == "Insufficient permissions"


class TestRolesPermitted:
    """Test roles_permitted function functionality."""

    def test_roles_permitted_with_matching_role(self):
        """Test roles_permitted when user has a matching role."""

        user_roles = [UserRole.admin, UserRole.mapper]
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is True

    def test_roles_permitted_with_no_matching_role(self):
        """Test roles_permitted when user has no matching roles."""

        user_roles = [UserRole.mapper]
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False

    def test_roles_permitted_with_empty_user_roles(self):
        """Test roles_permitted when user has no roles."""

        user_roles = []
        permitted_roles = [UserRole.admin]

        result = roles_permitted(user_roles, permitted_roles)
        assert result is False


class TestAssertPermission:
    """Test assert_permission function functionality."""

    def test_assert_permission_when_permitted(self):
        """Test assert_permission when permission is granted."""

        admin_data = Mock(user=Mock(id=1, username="1111-1111-1111-111X"), active_roles=[UserRole.admin])

        # Create a private score set that admin should have access to
        score_set = EntityTestHelper.create_score_set()

        # Should not raise exception
        result = assert_permission(admin_data, score_set, Action.READ)
        assert result.permitted is True

    def test_assert_permission_when_denied(self):
        """Test assert_permission when permission is denied - should raise PermissionException."""

        other_user_data = Mock(user=Mock(id=4, username="4444-4444-4444-444X"), active_roles=[])

        # Create a private score set that other user should not have access to
        score_set = EntityTestHelper.create_score_set()

        # Should raise PermissionException
        with pytest.raises(PermissionException) as exc_info:
            assert_permission(other_user_data, score_set, Action.READ)

        assert exc_info.value.http_code == 404
        assert "not found" in exc_info.value.message
