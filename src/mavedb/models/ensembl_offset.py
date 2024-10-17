from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.target_gene import TargetGene


class EnsemblOffset(Base):
    __tablename__ = "ensembl_offsets"

    identifier_id = Column(Integer, ForeignKey("ensembl_identifiers.id"), nullable=False, primary_key=True)
    identifier: Mapped[EnsemblIdentifier] = relationship(
        "EnsemblIdentifier", backref=backref("target_gene_offsets", uselist=True)
    )
    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, primary_key=True)
    target_gene: Mapped[TargetGene] = relationship(back_populates="ensembl_offset")

    offset = Column(Integer, nullable=False)
