from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User


def has_permission(user_data: Optional[UserData], entity: User, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on a User entity.

    This function evaluates user permissions based on user identity and roles.
    User entities have different access patterns since they don't have public/private
    states or ownership in the traditional sense.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The User entity to check permissions for.
        action: The action to be performed (READ, UPDATE, LOOKUP, ADD_ROLE).

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        NotImplementedError: If the action is not supported for User entities.

    Note:
        User entities do not have private/public states or traditional ownership models.
        Permissions are based on user identity and administrative roles.
    """
    user_is_self = False
    active_roles = []

    if user_data is not None:
        user_is_self = entity.id == user_data.user.id
        active_roles = user_data.active_roles

    save_to_logging_context(
        {
            "user_is_self": user_is_self,
            "target_user_id": entity.id,
        }
    )

    handlers = {
        Action.READ: _handle_read_action,
        Action.UPDATE: _handle_update_action,
        Action.LOOKUP: _handle_lookup_action,
        Action.ADD_ROLE: _handle_add_role_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for user profile entities. "
            f"Supported actions: {supported_actions}"
        )

    return handlers[action](
        user_data,
        entity,
        user_is_self,
        active_roles,
    )


def _handle_read_action(
    user_data: Optional[UserData],
    entity: User,
    user_is_self: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for User entities.

    Users can read their own profile. Admins can read any user profile.
    READ access to profiles refers to admin level properties. Basic user info
    is handled by the LOOKUP action.

    Args:
        user_data: The user's authentication data.
        entity: The User entity being accessed.
        user_is_self: Whether the user is viewing their own profile.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.

    Note:
        Basic user information (username, display name) is typically public,
        but sensitive information requires appropriate permissions.
    """
    ## Allow read access under the following conditions:
    # Users can always read their own profile.
    if user_is_self:
        return PermissionResponse(True)
    # Admins can read any user profile.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, False, user_data, False, "user profile")


def _handle_lookup_action(
    user_data: Optional[UserData],
    entity: User,
    user_is_self: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle LOOKUP action permission check for User entities.

    Any authenticated user can look up basic information about other users.
    Anonymous users cannot perform LOOKUP actions.

    Args:
        user_data: The user's authentication data.
        entity: The User entity being looked up.
        user_is_self: Whether the user is looking up their own profile.
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow lookup access under the following conditions:
    # Any authenticated user can look up basic user information.
    if user_data is not None and user_data.user is not None:
        return PermissionResponse(True)

    return deny_action_for_entity(entity, False, user_data, False, "user profile")


def _handle_update_action(
    user_data: Optional[UserData],
    entity: User,
    user_is_self: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for User entities.

    Users can update their own profile. Admins can update any user profile.

    Args:
        user_data: The user's authentication data.
        entity: The User entity being updated.
        user_is_self: Whether the user is updating their own profile.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # Users can update their own profile.
    if user_is_self:
        return PermissionResponse(True)
    # Admins can update any user profile.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, False, user_data, False, "user profile")


def _handle_add_role_action(
    user_data: Optional[UserData],
    entity: User,
    user_is_self: bool,
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_ROLE action permission check for User entities.

    Only admins can add roles to users.

    Args:
        user_data: The user's authentication data.
        entity: The User entity being modified.
        user_is_self: Whether the user is modifying their own profile.
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add role access under the following conditions:
    # Only admins can add roles to users.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, False, user_data, False, "user profile")
