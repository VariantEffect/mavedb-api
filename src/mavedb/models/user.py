from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from mavedb.db.base import Base
from mavedb.models.role import Role

users_roles_association_table = Table(
    "users_roles",
    Base.metadata,
    Column("user_id", ForeignKey("users.id"), primary_key=True),
    Column("role_id", ForeignKey("roles.id"), primary_key=True),
)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String, index=True, nullable=False)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    is_superuser = Column(Boolean, nullable=False, default=False)
    is_staff = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False)
    date_joined = Column(DateTime, nullable=True)
    email = Column(String, nullable=True)
    role_objs = relationship("Role", secondary=users_roles_association_table, backref="users")
    last_login = Column(DateTime, nullable=True)

    @property
    def roles(self) -> list[str]:
        role_objs = self.role_objs or []
        return list(map(lambda role_obj: role_obj.name, role_objs))

    async def set_roles(self, db, roles: list[str]):
        self.role_objs = [await self._find_or_create_role(db, name) for name in roles]

    @staticmethod
    async def _find_or_create_role(db, role_name):
        role_obj = db.query(Role).filter(Role.name == role_name).one_or_none()
        if not role_obj:
            role_obj = Role(name=role_name)
            # object_session.add(role_obj)
        return role_obj
