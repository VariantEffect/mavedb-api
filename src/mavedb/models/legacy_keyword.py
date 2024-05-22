from datetime import date

from sqlalchemy import Column, Date, Integer, String

from mavedb.db.base import Base


class LegacyKeyword(Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    text = Column(String, nullable=False, unique=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
