from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import roles_permitted
from mavedb.models.enums.user_role import UserRole
from mavedb.models.score_set import ScoreSet


def has_permission(user_data: Optional[UserData], entity: ScoreSet, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on a ScoreSet entity.

    This function evaluates user permissions based on ownership, contributor status,
    and user roles. It handles both private and public ScoreSets with different
    access control rules.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        action: The action to be performed (READ, UPDATE, DELETE, PUBLISH, SET_SCORES).
        entity: The ScoreSet entity to check permissions for.

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        ValueError: If the entity's private attribute is not set.
        NotImplementedError: If the action is not supported for ScoreSet entities.
    """
    if entity.private is None:
        raise ValueError("ScoreSet entity must have 'private' attribute set for permission checks.")

    user_is_owner = False
    user_is_contributor = False
    active_roles = []
    if user_data is not None:
        user_is_owner = entity.created_by_id == user_data.user.id
        user_is_contributor = user_data.user.username in [c.orcid_id for c in entity.contributors]
        active_roles = user_data.active_roles

    save_to_logging_context(
        {
            "resource_is_private": entity.private,
            "user_is_owner": user_is_owner,
            "user_is_contributor": user_is_contributor,
        }
    )

    handlers = {
        Action.READ: _handle_read_action,
        Action.UPDATE: _handle_update_action,
        Action.DELETE: _handle_delete_action,
        Action.PUBLISH: _handle_publish_action,
        Action.SET_SCORES: _handle_set_scores_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for ScoreSet entities. "
            f"Supported actions: {supported_actions}"
        )

    return handlers[action](
        user_data,
        entity,
        entity.private,
        user_is_owner,
        user_is_contributor,
        active_roles,
    )


def _handle_read_action(
    user_data: Optional[UserData],
    entity: ScoreSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for ScoreSet entities.

    Public ScoreSets are readable by anyone. Private ScoreSets are only readable
    by owners, contributors, admins, and mappers.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreSet entity being accessed.
        private: Whether the ScoreSet is private.
        user_is_owner: Whether the user owns the ScoreSet.
        user_is_contributor: Whether the user is a contributor to the ScoreSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow read access under the following conditions:
    # Any user may read a non-private score set.
    if not private:
        return PermissionResponse(True)
    # The owner or contributors may read a private score set.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may read a private score set.
    if roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
        return PermissionResponse(True)

    return _deny_action_for_score_set(entity, private, user_data, user_is_contributor or user_is_owner)


def _handle_update_action(
    user_data: Optional[UserData],
    entity: ScoreSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for ScoreSet entities.

    Only owners, contributors, and admins can update ScoreSets.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreSet entity being updated.
        private: Whether the ScoreSet is private.
        user_is_owner: Whether the user owns the ScoreSet.
        user_is_contributor: Whether the user is a contributor to the ScoreSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # The owner or contributors may update the score set.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may update the score set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return _deny_action_for_score_set(entity, private, user_data, user_is_contributor or user_is_owner)


def _handle_delete_action(
    user_data: Optional[UserData],
    entity: ScoreSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle DELETE action permission check for ScoreSet entities.

    Admins can delete any ScoreSet. Owners can only delete unpublished ScoreSets.
    Contributors cannot delete ScoreSets.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreSet entity being deleted.
        private: Whether the ScoreSet is private.
        user_is_owner: Whether the user owns the ScoreSet.
        user_is_contributor: Whether the user is a contributor to the ScoreSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow delete access under the following conditions:
    # Admins may delete any score set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may delete a score set only if it has not been published. Contributors may not delete a score set.
    if user_is_owner:
        published = not private
        return PermissionResponse(
            not published,
            403,
            f"insufficient permissions for URN '{entity.urn}'",
        )

    return _deny_action_for_score_set(entity, private, user_data, user_is_contributor or user_is_owner)


def _handle_publish_action(
    user_data: Optional[UserData],
    entity: ScoreSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle PUBLISH action permission check for ScoreSet entities.

    Owners, contributors, and admins can publish private ScoreSets to make them
    publicly accessible.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreSet entity being published.
        private: Whether the ScoreSet is private.
        user_is_owner: Whether the user owns the ScoreSet.
        user_is_contributor: Whether the user is a contributor to the ScoreSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow publish access under the following conditions:
    # The owner may publish the score set.
    if user_is_owner:
        return PermissionResponse(True)
    # Users with these specific roles may publish the score set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return _deny_action_for_score_set(entity, private, user_data, user_is_contributor or user_is_owner)


def _handle_set_scores_action(
    user_data: Optional[UserData],
    entity: ScoreSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle SET_SCORES action permission check for ScoreSet entities.

    Only owners, contributors, and admins can modify the scores data within
    a ScoreSet. This is a critical operation that affects the scientific data.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreSet entity whose scores are being modified.
        private: Whether the ScoreSet is private.
        user_is_owner: Whether the user owns the ScoreSet.
        user_is_contributor: Whether the user is a contributor to the ScoreSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow set scores access under the following conditions:
    # The owner or contributors may set scores.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may set scores.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return _deny_action_for_score_set(entity, private, user_data, user_is_contributor or user_is_owner)


def _deny_action_for_score_set(
    entity: ScoreSet,
    private: bool,
    user_data: Optional[UserData],
    user_may_view_private: bool,
) -> PermissionResponse:
    """
    Generate appropriate denial response for ScoreSet permission checks.

    This helper function determines the correct HTTP status code and message
    when denying access to a ScoreSet based on its privacy and user authentication.

    Args:
        entity: The ScoreSet entity being accessed.
        private: Whether the ScoreSet is private.
        user_data: The user's authentication data (None for anonymous).
        user_may_view_private: Whether the user has permission to view private ScoreSets.

    Returns:
        PermissionResponse: Denial response with appropriate HTTP status and message.

    Note:
        Returns 404 for private entities to avoid information disclosure,
        401 for unauthenticated users, and 403 for insufficient permissions.
    """
    # Do not acknowledge the existence of a private score set.
    if private and not user_may_view_private:
        return PermissionResponse(False, 404, f"score set with URN '{entity.urn}' not found")
    # No authenticated user is present.
    if user_data is None or user_data.user is None:
        return PermissionResponse(False, 401, f"insufficient permissions for URN '{entity.urn}'")

    # The authenticated user lacks sufficient permissions.
    return PermissionResponse(False, 403, f"insufficient permissions for URN '{entity.urn}'")
