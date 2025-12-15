from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment


def has_permission(user_data: Optional[UserData], entity: Experiment, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on an Experiment entity.

    This function evaluates user permissions based on ownership, contributor status,
    and user roles. It handles both private and public Experiments with different
    access control rules.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The Experiment entity to check permissions for.
        action: The action to be performed (READ, UPDATE, DELETE, ADD_SCORE_SET).

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        ValueError: If the entity's private attribute is not set.
        NotImplementedError: If the action is not supported for Experiment entities.
    """
    if entity.private is None:
        raise ValueError("Experiment entity must have 'private' attribute set for permission checks.")

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
        Action.ADD_SCORE_SET: _handle_add_score_set_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for experiment entities. "
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
    entity: Experiment,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for Experiment entities.

    Public Experiments are readable by anyone. Private Experiments are only readable
    by owners, contributors, admins, and mappers.

    Args:
        user_data: The user's authentication data.
        entity: The Experiment entity being accessed.
        private: Whether the Experiment is private.
        user_is_owner: Whether the user owns the Experiment.
        user_is_contributor: Whether the user is a contributor to the Experiment.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow read access under the following conditions:
    # Any user may read a non-private experiment.
    if not private:
        return PermissionResponse(True)
    # The owner or contributors may read a private experiment.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may read a private experiment.
    if roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment")


def _handle_update_action(
    user_data: Optional[UserData],
    entity: Experiment,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for Experiment entities.

    Only owners, contributors, and admins can update Experiments.

    Args:
        user_data: The user's authentication data.
        entity: The Experiment entity being updated.
        private: Whether the Experiment is private.
        user_is_owner: Whether the user owns the Experiment.
        user_is_contributor: Whether the user is a contributor to the Experiment.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # The owner or contributors may update the experiment.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may update the experiment.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment")


def _handle_delete_action(
    user_data: Optional[UserData],
    entity: Experiment,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle DELETE action permission check for Experiment entities.

    Admins can delete any Experiment. Owners can only delete unpublished Experiments.
    Contributors cannot delete Experiments.

    Args:
        user_data: The user's authentication data.
        entity: The Experiment entity being deleted.
        private: Whether the Experiment is private.
        user_is_owner: Whether the user owns the Experiment.
        user_is_contributor: Whether the user is a contributor to the Experiment.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow delete access under the following conditions:
    # Admins may delete any experiment.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may delete an experiment only if it is still private. Contributors may not delete an experiment.
    if user_is_owner and private:
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment")


def _handle_add_score_set_action(
    user_data: Optional[UserData],
    entity: Experiment,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_SCORE_SET action permission check for Experiment entities.

    Only permitted users can add a score set to a private experiment.
    Any authenticated user can add a score set to a public experiment.

    Args:
        user_data: The user's authentication data.
        entity: The Experiment entity to add a score set to.
        private: Whether the Experiment is private.
        user_is_owner: Whether the user owns the Experiment.
        user_is_contributor: Whether the user is a contributor to the Experiment.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add score set access under the following conditions:
    # Owners or contributors may add a score set.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may update the experiment.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Any authenticated user may add a score set to a non-private experiment.
    if not private and user_data is not None:
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment")
