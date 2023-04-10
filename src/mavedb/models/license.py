from datetime import date

from sqlalchemy import Column, Date, Integer, String

from mavedb.db.base import Base


class License(Base):
    __tablename__ = "licenses"

    id = Column(Integer, primary_key=True, index=True)
    long_name = Column(String, nullable=False, unique=True)
    short_name = Column(String, nullable=False, unique=True)
    text = Column(String, nullable=False, unique=False)
    link = Column(String, nullable=True, unique=False)
    version = Column(String, nullable=True, unique=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
