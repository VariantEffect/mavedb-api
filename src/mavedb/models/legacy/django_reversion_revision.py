from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func

from mavedb.db.base import Base


class DjangoReversionRevision(Base):
    __tablename__ = "reversion_revision"

    id = Column(Integer, primary_key=True, index=True)
    date_created = Column(DateTime(), default=func.now(), nullable=False)
    comment = Column(String, nullable=False)
    user_id = Column(Integer, nullable=True)
