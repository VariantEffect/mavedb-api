from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base


class ReferenceMap(Base):
    __tablename__ = "reference_maps"

    id = Column(Integer, primary_key=True, index=True)
    is_primary = Column(Boolean, nullable=False, default=False)
    genome_id = Column(Integer, ForeignKey("reference_genomes.id"), nullable=False)
    genome = relationship("ReferenceGenome", backref="reference_maps")
    target_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False)
    target = relationship("TargetGene", backref=backref("reference_maps", cascade="all,delete-orphan"))
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
