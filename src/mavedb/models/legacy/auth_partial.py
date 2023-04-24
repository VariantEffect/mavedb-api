from sqlalchemy import Boolean, Column, DateTime, Integer, String

from mavedb.db.base import Base


class AuthCode(Base):
    __tablename__ = "social_auth_code"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(254), nullable=False)
    code = Column(String(32), nullable=False)
    verified = Column(Boolean, nullable=False)
    date_created = Column(DateTime(timestamp=True), nullable=False)
