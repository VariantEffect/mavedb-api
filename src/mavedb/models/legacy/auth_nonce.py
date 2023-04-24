from sqlalchemy import Column, Integer, String

from mavedb.db.base import Base


class AuthNonce(Base):
    __tablename__ = "social_auth_nonce"

    id = Column(Integer, primary_key=True, index=True)
    server_url = Column(String(255), nullable=False)
    timestamp = Column(Integer, nullable=False)
    salt = Column(String(65), nullable=False)
