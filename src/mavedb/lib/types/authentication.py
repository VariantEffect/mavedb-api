from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mavedb.models.enums.user_role import UserRole
    from mavedb.models.user import User


@dataclass
class UserData:
    user: "User"
    active_roles: list["UserRole"]
