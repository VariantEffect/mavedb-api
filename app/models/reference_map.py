from datetime import date
from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer
from sqlalchemy.orm import relationship

from app.db.base import Base
from .reference_genome import ReferenceGenome
# from .target_gene import TargetGene


class ReferenceMap(Base):
    __tablename__ = "genome_referencemap"

    id = Column(Integer, primary_key=True, index=True)
    is_primary = Column(Boolean, nullable=False, default=False)
    genome_id = Column(Integer, ForeignKey("genome_referencegenome.id"), nullable=False)
    genome = relationship("ReferenceGenome", back_populates="reference_maps")
    target_id = Column(Integer, ForeignKey("genome_targetgene.id"), nullable=False)
    target = relationship("TargetGene", back_populates="reference_maps")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
