from sqlalchemy import Column, Integer, String

from mavedb.db.base import Base


class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True, nullable=False)
