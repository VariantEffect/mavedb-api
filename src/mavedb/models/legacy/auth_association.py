from sqlalchemy import Column, Integer, String

from mavedb.db.base import Base


class AuthAssociation(Base):
    __tablename__ = "social_auth_association"

    id = Column(Integer, primary_key=True, index=True)
    server_url = Column(String(255), nullable=False)
    handle = Column(String(255), nullable=False)
    secret = Column(String(255), nullable=False)
    issued = Column(Integer, nullable=False)
    lifetime = Column(Integer, nullable=False)
    assoc_type = Column(String(64), nullable=True)
