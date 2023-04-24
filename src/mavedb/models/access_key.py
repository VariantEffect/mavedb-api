from datetime import datetime

from sqlalchemy import Column, Date, DateTime, Integer, String, ForeignKey
from sqlalchemy.orm import backref, relationship

from mavedb.db.base import Base


class AccessKey(Base):
    __tablename__ = "access_keys"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    user = relationship("User", backref=backref("access_keys", cascade="all,delete-orphan"))
    key_id = Column(String, unique=True, index=True, nullable=False)
    public_key = Column(String, nullable=False)
    name = Column(String, nullable=True)
    expiration_date = Column(Date, nullable=True)
    creation_time = Column(DateTime, nullable=True, default=datetime.now)
