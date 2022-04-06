from datetime import date
from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.db.base import Base
from .reference_map import ReferenceMap


class TargetGene(Base):
    __tablename__ = "genome_targetgene"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(256), nullable=False)
    category = Column(String(32), nullable=False)

    ensembl_id_id = Column(Integer, nullable=True)  # , ForeignKey("dataset_scoreset.id"), nullable=False)
    refseq_id_id = Column(Integer, nullable=True)  # , ForeignKey("dataset_scoreset.id"), nullable=False)
    scoreset_id = Column(Integer, ForeignKey("dataset_scoreset.id"), nullable=False)
    scoreset = relationship("Scoreset", back_populates="target_gene")
    uniprot_id_id = Column(Integer, nullable=True)  # , ForeignKey("dataset_scoreset.id"), nullable=False)
    wt_sequence_id = Column(Integer, nullable=False)  # , ForeignKey("dataset_scoreset.id"), nullable=False)

    reference_maps = relationship("ReferenceMap", back_populates="target")

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
