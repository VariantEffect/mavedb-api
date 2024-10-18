from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.role import Role
from mavedb.models.user import User

if TYPE_CHECKING:
    from mavedb.lib.authorization import UserRole


class AccessKey(Base):
    __tablename__ = "access_keys"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    user: Mapped[User] = relationship(back_populates="access_keys")
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=True)
    role_obj: Mapped[Role] = relationship(Role)
    key_id = Column(String, unique=True, index=True, nullable=False)
    public_key = Column(String, nullable=False)
    name = Column(String, nullable=True)
    expiration_date = Column(Date, nullable=True)
    creation_time = Column(DateTime, nullable=True, default=datetime.now)

    @property
    def role(self) -> Optional["UserRole"]:
        role_obj = self.role_obj or None
        return None if role_obj is None else role_obj.name

    async def set_role(self, db, role: "UserRole"):
        self.role_obj = await self._find_or_create_role(db, role)

    @staticmethod
    async def _find_or_create_role(db, role):
        role_obj = db.query(Role).filter(Role.name == role).one_or_none()
        if not role_obj:
            role_obj = Role(name=role)

        return role_obj
