from datetime import date

from sqlalchemy import Column, Date, Integer, String

from mavedb.db.base import Base


class EnsemblIdentifier(Base):
    __tablename__ = "ensembl_identifiers"

    id = Column(Integer, primary_key=True)
    identifier = Column(String, nullable=False)
    db_name = Column(String, nullable=False)
    db_version = Column(String, nullable=True)
    url = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
