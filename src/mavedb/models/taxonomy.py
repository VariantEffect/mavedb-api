from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

from .genome_identifier import GenomeIdentifier


class Taxonomy(Base):
    __tablename__ = "taxonomies"

    id = Column(Integer, primary_key=True)
    tax_id = Column(Integer, nullable=False)
    organism_name = Column(String, nullable=True)
    common_name = Column(String, nullable=True)
    rank = Column(String, nullable=True)
    has_described_species_name = Column(Boolean, nullable=True)
    url = Column(String, nullable=False)
    article_reference = Column(String, nullable=True)
    genome_identifier_id = Column(Integer, ForeignKey("genome_identifiers.id"), index=True, nullable=True)
    genome_identifier: Mapped[GenomeIdentifier] = relationship("GenomeIdentifier", backref="taxonomy_genomes")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
