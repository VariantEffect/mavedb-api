from typing import Optional

from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
from mavedb.lib.types.authentication import UserData
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment_set import ExperimentSet


def has_permission(user_data: Optional[UserData], entity: ExperimentSet, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on an ExperimentSet entity.

    This function evaluates user permissions based on ownership, contributor status,
    and user roles. It handles both private and public ExperimentSets with different
    access control rules.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The ExperimentSet entity to check permissions for.
        action: The action to be performed (READ, UPDATE, DELETE, ADD_EXPERIMENT).

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        ValueError: If the entity's private attribute is not set.
        NotImplementedError: If the action is not supported for ExperimentSet entities.
    """
    if entity.private is None:
        raise ValueError("ExperimentSet entity must have 'private' attribute set for permission checks.")

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
        Action.ADD_EXPERIMENT: _handle_add_experiment_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for experiment set entities. "
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
    entity: ExperimentSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for ExperimentSet entities.

    Public ExperimentSets are readable by anyone. Private ExperimentSets are only readable
    by owners, contributors, admins, and mappers.

    Args:
        user_data: The user's authentication data.
        entity: The ExperimentSet entity being accessed.
        private: Whether the ExperimentSet is private.
        user_is_owner: Whether the user owns the ExperimentSet.
        user_is_contributor: Whether the user is a contributor to the ExperimentSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow read access under the following conditions:
    # Any user may read a non-private experiment set.
    if not private:
        return PermissionResponse(True)
    # The owner or contributors may read a private experiment set.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may read a private experiment set.
    if roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment set")


def _handle_update_action(
    user_data: Optional[UserData],
    entity: ExperimentSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for ExperimentSet entities.

    Only owners, contributors, and admins can update ExperimentSets.

    Args:
        user_data: The user's authentication data.
        entity: The ExperimentSet entity being updated.
        private: Whether the ExperimentSet is private.
        user_is_owner: Whether the user owns the ExperimentSet.
        user_is_contributor: Whether the user is a contributor to the ExperimentSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # The owner or contributors may update the experiment set.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may update the experiment set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment set")


def _handle_delete_action(
    user_data: Optional[UserData],
    entity: ExperimentSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle DELETE action permission check for ExperimentSet entities.

    Admins can delete any ExperimentSet. Owners can only delete unpublished ExperimentSets.
    Contributors cannot delete ExperimentSets.

    Args:
        user_data: The user's authentication data.
        entity: The ExperimentSet entity being deleted.
        private: Whether the ExperimentSet is private.
        user_is_owner: Whether the user owns the ExperimentSet.
        user_is_contributor: Whether the user is a contributor to the ExperimentSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow delete access under the following conditions:
    # Admins may delete any experiment set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Owners may delete an experiment set only if it is still private. Contributors may not delete an experiment set.
    if user_is_owner and private:
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment set")


def _handle_add_experiment_action(
    user_data: Optional[UserData],
    entity: ExperimentSet,
    private: bool,
    user_is_owner: bool,
    user_is_contributor: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_EXPERIMENT action permission check for ExperimentSet entities.

    Only permitted users can add an experiment to a private experiment set.
    Any authenticated user can add an experiment to a public experiment set.

    Args:
        user_data: The user's authentication data.
        entity: The ExperimentSet entity to add an experiment to.
        private: Whether the ExperimentSet is private.
        user_is_owner: Whether the user owns the ExperimentSet.
        user_is_contributor: Whether the user is a contributor to the ExperimentSet.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add experiment access under the following conditions:
    # Owners or contributors may add an experiment.
    if user_is_owner or user_is_contributor:
        return PermissionResponse(True)
    # Users with these specific roles may add an experiment to the experiment set.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, user_is_contributor or user_is_owner, "experiment set")
