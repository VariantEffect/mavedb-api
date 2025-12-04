from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
from mavedb.models.enums.user_role import UserRole
from mavedb.models.score_calibration import ScoreCalibration


def has_permission(user_data: Optional[UserData], entity: ScoreCalibration, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on a ScoreCalibration entity.

    This function evaluates user permissions for ScoreCalibration entities, which are
    typically administrative objects that require special permissions to modify.
    ScoreCalibrations don't have traditional ownership but are tied to ScoreSets.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The ScoreCalibration entity to check permissions for.
        action: The action to be performed (READ, UPDATE, DELETE, CREATE).

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        NotImplementedError: If the action is not supported for ScoreCalibration entities.
    """
    if entity.private is None:
        raise ValueError("ScoreCalibration entity must have 'private' attribute set for permission checks.")

    user_is_owner = False
    user_is_contributor_to_score_set = False
    active_roles = []
    if user_data is not None:
        user_is_owner = entity.created_by_id == user_data.user.id
        # Contributor status is determined by matching the user's username (ORCID ID) against the contributors' ORCID IDs,
        # as well as by matching the user's ID against the created_by_id and modified_by_id fields of the ScoreSet.
        user_is_contributor_to_score_set = (
            user_data.user.username in [c.orcid_id for c in entity.score_set.contributors]
            or user_data.user.id == entity.score_set.created_by_id
            or user_data.user.id == entity.score_set.modified_by_id
        )
        active_roles = user_data.active_roles

    save_to_logging_context(
        {
            "user_is_owner": user_is_owner,
            "user_is_contributor_to_score_set": user_is_contributor_to_score_set,
            "score_calibration_id": entity.id,
        }
    )

    handlers = {
        Action.READ: _handle_read_action,
        Action.UPDATE: _handle_update_action,
        Action.DELETE: _handle_delete_action,
        Action.PUBLISH: _handle_publish_action,
        Action.CHANGE_RANK: _handle_change_rank_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for ScoreCalibration entities. "
            f"Supported actions: {supported_actions}"
        )

    return handlers[action](
        user_data,
        entity,
        user_is_owner,
        user_is_contributor_to_score_set,
        entity.private,
        active_roles,
    )


def _handle_read_action(
    user_data: Optional[UserData],
    entity: ScoreCalibration,
    user_is_owner: bool,
    user_is_contributor_to_score_set: bool,
    private: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for ScoreCalibration entities.

    ScoreCalibrations are generally readable by anyone who can access the
    associated ScoreSet, as they provide important contextual information
    about the score data.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreCalibration entity being accessed.
        user_is_owner: Whether the user created the ScoreCalibration.
        user_is_contributor_to_score_set: Whether the user is a contributor to the associated ScoreSet.
        private: Whether the ScoreCalibration is private.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow read access under the following conditions:
    # Any user may read a ScoreCalibration if it is not private.
    if not private:
        return PermissionResponse(True)
    # Owners of the ScoreCalibration may read it.
    if user_is_owner:
        return PermissionResponse(True)
    # If the calibration is investigator provided, contributors to the ScoreSet may read it.
    if entity.investigator_provided and user_is_contributor_to_score_set:
        return PermissionResponse(True)
    # System admins may read any ScoreCalibration.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    user_may_view_private = user_is_owner or (entity.investigator_provided and user_is_contributor_to_score_set)
    return deny_action_for_entity(entity, private, user_data, user_may_view_private)


def _handle_update_action(
    user_data: Optional[UserData],
    entity: ScoreCalibration,
    user_is_owner: bool,
    user_is_contributor_to_score_set: bool,
    private: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for ScoreCalibration entities.

    Updating ScoreCalibrations is typically restricted to administrators
    or the original creators, as changes can significantly impact
    the interpretation of score data.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreCalibration entity being accessed.
        user_is_owner: Whether the user crated the ScoreCalibration.
        user_is_contributor_to_score_set: Whether the user is a contributor to the associated ScoreSet.
        private: Whether the ScoreCalibration is private.
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # System admins may update any ScoreCalibration.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # TODO#549: Allow editing of certain fields if the calibration is published.
    #           For now, published calibrations cannot be updated.
    if entity.private:
        # Owners may update their own ScoreCalibration if it is not published.
        if user_is_owner:
            return PermissionResponse(True)
        # If the calibration is investigator provided, contributors to the ScoreSet may update it if not published.
        if entity.investigator_provided and user_is_contributor_to_score_set:
            return PermissionResponse(True)

    user_may_view_private = user_is_owner or (entity.investigator_provided and user_is_contributor_to_score_set)
    return deny_action_for_entity(entity, private, user_data, user_may_view_private)


def _handle_delete_action(
    user_data: Optional[UserData],
    entity: ScoreCalibration,
    user_is_owner: bool,
    user_is_contributor_to_score_set: bool,
    private: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle DELETE action permission check for ScoreCalibration entities.

    Deleting ScoreCalibrations is a sensitive operation typically reserved
    for administrators or the original creators, as it can affect data integrity.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreCalibration entity being accessed.
        user_is_owner: Whether the user created the ScoreCalibration.
        user_is_contributor_to_score_set: Whether the user is a contributor to the associated ScoreSet.
        private: Whether the ScoreCalibration is private.
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow delete access under the following conditions:
    # System admins may delete any ScoreCalibration.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may delete their own ScoreCalibration if it is still private. Contributors may not delete ScoreCalibrations.
    if user_is_owner and private:
        return PermissionResponse(True)

    user_may_view_private = user_is_owner or (entity.investigator_provided and user_is_contributor_to_score_set)
    return deny_action_for_entity(entity, private, user_data, user_may_view_private)


def _handle_publish_action(
    user_data: Optional[UserData],
    entity: ScoreCalibration,
    user_is_owner: bool,
    user_is_contributor_to_score_set: bool,
    private: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle PUBLISH action permission check for ScoreCalibration entities.

    Publishing ScoreCalibrations is typically restricted to administrators
    or the original creators, as it signifies that the calibration is
    finalized and ready for public use.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreCalibration entity being accessed.
        user_is_owner: Whether the user created the ScoreCalibration.
        user_is_contributor_to_score_set: Whether the user is a contributor to the associated ScoreSet.
        private: Whether the ScoreCalibration is private.
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow publish access under the following conditions:
    # System admins may publish any ScoreCalibration.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may publish their own ScoreCalibration.
    if user_is_owner:
        return PermissionResponse(True)

    user_may_view_private = user_is_owner or (entity.investigator_provided and user_is_contributor_to_score_set)
    return deny_action_for_entity(entity, private, user_data, user_may_view_private)


def _handle_change_rank_action(
    user_data: Optional[UserData],
    entity: ScoreCalibration,
    user_is_owner: bool,
    user_is_contributor_to_score_set: bool,
    private: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle CHANGE_RANK action permission check for ScoreCalibration entities.

    Changing the rank of ScoreCalibrations is typically restricted to administrators
    or the original creators, as it affects the order in which calibrations are applied.

    Args:
        user_data: The user's authentication data.
        entity: The ScoreCalibration entity being accessed.
        user_is_owner: Whether the user created the ScoreCalibration.
        user_is_contributor_to_score_set: Whether the user is a contributor to the associated ScoreSet.
        private: Whether the ScoreCalibration is private.
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow change rank access under the following conditions:
    # System admins may change the rank of any ScoreCalibration.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may change the rank of their own ScoreCalibration.
    if user_is_owner:
        return PermissionResponse(True)
    # If the calibration is investigator provided, contributors to the ScoreSet may change its rank.
    if entity.investigator_provided and user_is_contributor_to_score_set:
        return PermissionResponse(True)

    user_may_view_private = user_is_owner or (entity.investigator_provided and user_is_contributor_to_score_set)
    return deny_action_for_entity(entity, private, user_data, user_may_view_private)
