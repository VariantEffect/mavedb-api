from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.target_gene import TargetGene


class RefseqOffset(Base):
    __tablename__ = "refseq_offsets"

    identifier_id = Column(Integer, ForeignKey("refseq_identifiers.id"), nullable=False, primary_key=True)
    identifier: Mapped[RefseqIdentifier] = relationship(backref=backref("target_gene_offsets", uselist=True))
    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, primary_key=True)
    target_gene: Mapped[TargetGene] = relationship(back_populates="refseq_offset", single_parent=True)
    offset = Column(Integer, nullable=False)
