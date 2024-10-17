from sqlalchemy import Column, Enum, Integer

from mavedb.db.base import Base
from mavedb.models.enums.user_role import UserRole


class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True)
    name = Column(
        Enum(UserRole, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=False,
    )
