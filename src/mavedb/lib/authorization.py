from typing import Optional

from fastapi import Depends, HTTPException
from starlette import status

from mavedb.lib.authentication import get_current_user
from mavedb.models.user import User

ROLES = {"admin": "admin"}


####################################################################################################
# Main authorization methods
####################################################################################################


async def require_current_user(user: Optional[User] = Depends(get_current_user)) -> User:
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")
    user_value: User = user
    return user_value


class RoleRequirer:
    def __init__(self, roles: list[str]):
        self.roles = roles

    async def __call__(self, user: User = Depends(require_current_user)):
        if not any(x in user.roles for x in self.roles):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="You are not authorized to use this feature"
            )
        return user


async def require_role(roles: list[str], user: User = Depends(require_current_user)) -> User:
    if not any(x in user.roles for x in roles):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="You are not authorized to use this feature"
        )
    return user
