from sqlalchemy import Column, DateTime, Integer, String

from mavedb.db.base import Base


class DjangoReversionRevision(Base):
    __tablename__ = "reversion_revision"

    id = Column(Integer, primary_key=True, index=True)
    date_created = Column(DateTime(timestamp=True), nullable=False)
    comment = Column(String, nullable=False)
    user_id = Column(Integer, nullable=True)
