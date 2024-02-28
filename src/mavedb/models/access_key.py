from datetime import datetime

from sqlalchemy import Column, Date, DateTime, Integer, String, ForeignKey
from sqlalchemy.orm import backref, relationship, Mapped

from mavedb.db.base import Base
from mavedb.models.user import User


class AccessKey(Base):
    __tablename__ = "access_keys"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    user : Mapped[User] = relationship(back_populates="access_keys")
    key_id = Column(String, unique=True, index=True, nullable=False)
    public_key = Column(String, nullable=False)
    name = Column(String, nullable=True)
    expiration_date = Column(Date, nullable=True)
    creation_time = Column(DateTime, nullable=True, default=datetime.now)
