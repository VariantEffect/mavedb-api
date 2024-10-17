from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Table
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.enums.user_role import UserRole
from mavedb.models.role import Role

users_roles_association_table = Table(
    "users_roles",
    Base.metadata,
    Column("user_id", ForeignKey("users.id"), primary_key=True),
    Column("role_id", ForeignKey("roles.id"), primary_key=True),
)

if TYPE_CHECKING:
    from mavedb.models.access_key import AccessKey


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String, index=True, nullable=False, unique=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    is_superuser = Column(Boolean, nullable=False, default=False)
    is_staff = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False)
    date_joined = Column(DateTime, nullable=True)
    email = Column(String, nullable=True)
    role_objs: Mapped[list[Role]] = relationship("Role", secondary=users_roles_association_table, backref="users")
    is_first_login: Mapped[bool] = Column(Boolean, nullable=False, default=True)
    last_login = Column(DateTime, nullable=True)

    access_keys: Mapped[list["AccessKey"]] = relationship(back_populates="user", cascade="all, delete-orphan")

    @property
    def roles(self) -> list[UserRole]:
        role_objs = self.role_objs or []
        return [role_obj.name for role_obj in role_objs if role_obj.name is not None]

    async def set_roles(self, db, roles: list[UserRole]):
        self.role_objs = [await self._find_or_create_role(db, name) for name in roles]

    @staticmethod
    async def _find_or_create_role(db, role_name):
        role_obj = db.query(Role).filter(Role.name == role_name).one_or_none()
        if not role_obj:
            role_obj = Role(name=role_name)
            # object_session.add(role_obj)
        return role_obj
