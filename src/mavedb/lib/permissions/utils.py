import logging
from typing import Union, overload

from mavedb.lib.logging.context import logging_context, save_to_logging_context
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
