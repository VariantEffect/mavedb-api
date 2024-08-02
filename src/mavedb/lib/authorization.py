import logging
from typing import Optional

from fastapi import Depends, HTTPException
from starlette import status

from mavedb.lib.authentication import get_current_user, UserData
from mavedb.lib.logging.context import dump_context, save_to_context
from mavedb.models.enums.user_role import UserRole


logger = logging.getLogger(__name__)


####################################################################################################
# Main authorization methods
####################################################################################################


async def require_current_user(
    user_data: Optional[UserData] = Depends(get_current_user),
) -> UserData:
    if user_data is None:
        logger.info(dump_context(message="Non-authenticated user attempted to access protected route."))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        )

    return user_data


async def require_current_user_with_email(
    user_data: UserData = Depends(require_current_user),
) -> UserData:
    # Both empty strings and NoneType values should raise an exception.
    if not user_data.user.email:
        logger.info(dump_context(message="User attempted to access email protected route without a valid email."))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There must be an email address associated with your account to use this feature.",
        )
    return user_data


class RoleRequirer:
    def __init__(self, roles: list[UserRole]):
        self.roles = roles

    async def __call__(self, user_data: UserData = Depends(require_current_user)) -> UserData:
        save_to_context({"required_roles": [role.name for role in self.roles]})
        if not any(role in self.roles for role in user_data.active_roles):
            logger.info(dump_context(message="User attempted to access role protected route without a required role."))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="You are not authorized to use this feature",
            )

        return user_data


async def require_role(roles: list[UserRole], user_data: UserData = Depends(require_current_user)) -> UserData:
    save_to_context({"required_roles": [role.name for role in roles]})
    if not any(role.name in roles for role in user_data.active_roles):
        logger.info(dump_context(message="User attempted to access role protected route without a required role."))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not authorized to use this feature",
        )

    return user_data
