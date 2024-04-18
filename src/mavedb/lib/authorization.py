from typing import Optional

from fastapi import Depends, HTTPException
from starlette import status

from mavedb.lib.authentication import get_current_user, UserData
from mavedb.models.enums.user_role import UserRole


####################################################################################################
# Main authorization methods
####################################################################################################


async def require_current_user(user_data: Optional[UserData] = Depends(get_current_user)) -> UserData:
    if user_data is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")

    return user_data


class RoleRequirer:
    def __init__(self, roles: list[UserRole]):
        self.roles = roles

    async def __call__(self, user_data: UserData = Depends(require_current_user)) -> UserData:
        if not any(role in self.roles for role in user_data.active_roles):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="You are not authorized to use this feature"
            )

        return user_data


async def require_role(roles: list[UserRole], user_data: UserData = Depends(require_current_user)) -> UserData:
    if not any(role.name in roles for role in user_data.active_roles):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="You are not authorized to use this feature"
        )

    return user_data
