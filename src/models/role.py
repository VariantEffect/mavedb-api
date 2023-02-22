from sqlalchemy import Column, Integer, String

from src.db.base import Base


class Role(Base):
    __tablename__ = 'roles'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True, nullable=False)
