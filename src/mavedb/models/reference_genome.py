from datetime import date

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from mavedb.db.base import Base
from .genome_identifier import GenomeIdentifier


class ReferenceGenome(Base):
    __tablename__ = "reference_genomes"

    id = Column(Integer, primary_key=True, index=True)
    short_name = Column(String, nullable=False)
    organism_name = Column(String, nullable=False)
    genome_identifier_id = Column(Integer, ForeignKey("genome_identifiers.id"), nullable=True)
    genome_identifier = relationship("GenomeIdentifier", backref="reference_genomes")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
