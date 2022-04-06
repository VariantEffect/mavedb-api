from datetime import date
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.orm import relationship

from app.db.base import Base


class ReferenceGenome(Base):
    __tablename__ = "genome_referencegenome"

    id = Column(Integer, primary_key=True, index=True)
    short_name = Column(String(256), nullable=False)
    organism_name = Column(String(256), nullable=False)
    genome_id_id = Column(Integer, nullable=True)  #, ForeignKey("target_gene.id"), nullable=True)
    # genome_id = relationship("Genome", back_populates="reference_maps")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    reference_maps = relationship("ReferenceMap", back_populates="genome")
