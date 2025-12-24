import logging
from typing import Optional, Union, overload

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.types.authentication import UserData
from mavedb.lib.types.permissions import EntityType
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole

logger = logging.getLogger(__name__)


@overload
def roles_permitted(
    user_roles: list[UserRole],
    permitted_roles: list[UserRole],
) -> bool: ...


@overload
def roles_permitted(
    user_roles: list[ContributionRole],
    permitted_roles: list[ContributionRole],
) -> bool: ...


def roles_permitted(
    user_roles: Union[list[UserRole], list[ContributionRole]],
    permitted_roles: Union[list[UserRole], list[ContributionRole]],
) -> bool:
    """
    Check if any user role is permitted based on a list of allowed roles.

    This function validates that both user_roles and permitted_roles are lists of the same enum type
    (either all UserRole or all ContributionRole), and checks if any user role is present in the permitted roles.
    Raises ValueError if either list contains mixed role types or if the lists are of different types.

    Args:
        user_roles: List of roles assigned to the user (UserRole or ContributionRole).
        permitted_roles: List of roles that are permitted for the action (UserRole or ContributionRole).

    Returns:
        bool: True if any user role is permitted, False otherwise.

    Raises:
        ValueError: If user_roles or permitted_roles contain mixed role types, or if the lists are of different types.

    Example:
        >>> roles_permitted([UserRole.admin], [UserRole.admin, UserRole.editor])
        True
        >>> roles_permitted([ContributionRole.admin], [ContributionRole.editor])
        False

    Note:
        This function is used to enforce type safety and prevent mixing of role enums in permission checks.
    """
    save_to_logging_context({"permitted_roles": [role.name for role in permitted_roles]})

    if not user_roles:
        logger.debug(msg="User has no associated roles.", extra=logging_context())
        return False

    # Validate that both lists contain the same enum type
    if user_roles and permitted_roles:
        user_role_types = {type(role) for role in user_roles}
        permitted_role_types = {type(role) for role in permitted_roles}

        # Check if either list has mixed types
        if len(user_role_types) > 1:
            raise ValueError("user_roles list cannot contain mixed role types (UserRole and ContributionRole)")
        if len(permitted_role_types) > 1:
            raise ValueError("permitted_roles list cannot contain mixed role types (UserRole and ContributionRole)")

        # Check if the lists have different role types
        if user_role_types != permitted_role_types:
            raise ValueError(
                "user_roles and permitted_roles must contain the same role type (both UserRole or both ContributionRole)"
            )

    return any(role in permitted_roles for role in user_roles)


def deny_action_for_entity(
    entity: EntityType,
    private: bool,
    user_data: Optional[UserData],
    user_may_view_private: bool,
    user_facing_model_name: str = "entity",
) -> PermissionResponse:
    """
    Generate appropriate denial response for entity permission checks.

    This helper function determines the correct HTTP status code and message
    when denying access to an entity based on its privacy and user authentication.

    Args:
        entity: The entity being accessed.
        private: Whether the entity is private.
        user_data: The user's authentication data (None for anonymous).
        user_may_view_private: Whether the user has permission to view private entities.

    Returns:
        PermissionResponse: Denial response with appropriate HTTP status and message.

    Note:
        Returns 404 for private entities to avoid information disclosure,
        401 for unauthenticated users, and 403 for insufficient permissions.
    """

    def _identifier_for_entity(entity: EntityType) -> tuple[str, str]:
        if hasattr(entity, "urn") and entity.urn is not None:
            return "URN", entity.urn
        elif hasattr(entity, "id") and entity.id is not None:
            return "ID", str(entity.id)
        else:
            return "unknown", "unknown"

    field, identifier = _identifier_for_entity(entity)
    # Do not acknowledge the existence of a private score set.
    if private and not user_may_view_private:
        return PermissionResponse(False, 404, f"{user_facing_model_name} with {field} '{identifier}' not found")
    # No authenticated user is present.
    if user_data is None or user_data.user is None:
        return PermissionResponse(
            False, 401, f"authentication required to access {user_facing_model_name} with {field} '{identifier}'"
        )

    # The authenticated user lacks sufficient permissions.
    return PermissionResponse(
        False, 403, f"insufficient permissions on {user_facing_model_name} with {field} '{identifier}'"
    )
