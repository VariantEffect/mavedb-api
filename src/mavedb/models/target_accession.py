from datetime import date

from sqlalchemy import Column, Date, Integer, String

from mavedb.db.base import Base


class TargetAccession(Base):
    __tablename__ = "target_accessions"

    id = Column(Integer, primary_key=True)
    assembly = Column(String, nullable=True)
    accession = Column(String, nullable=False)
    gene = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
