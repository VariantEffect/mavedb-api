from datetime import date

from sqlalchemy import Column, Date, Integer, String, ForeignKey
from sqlalchemy.orm import backref, relationship

from mavedb.db.base import Base


class TargetAccession(Base):
    __tablename__ = "target_accessions"

    id = Column(Integer, primary_key=True, index=True)
    accession = Column(String, nullable=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)