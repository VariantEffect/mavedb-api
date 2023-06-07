from datetime import date

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base
from .genome_identifier import GenomeIdentifier


class Taxonomy(Base):
    __tablename__ = "taxonomy"

    id = Column(Integer, primary_key=True, index=True)
    tax_id = Column(Integer, nullable=False)
    species_name = Column(String, nullable=False)
    common_name = Column(String, nullable=False)
    genome_identifier_id = Column(Integer, ForeignKey("genome_identifiers.id"), nullable=True)
    genome_identifier = relationship("GenomeIdentifier", backref="taxonomy_genomes")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

