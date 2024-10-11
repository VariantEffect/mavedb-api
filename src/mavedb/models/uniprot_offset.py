from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base
from mavedb.models.target_gene import TargetGene
from mavedb.models.uniprot_identifier import UniprotIdentifier


class UniprotOffset(Base):
    __tablename__ = "uniprot_offsets"

    identifier_id = Column(Integer, ForeignKey("uniprot_identifiers.id"), nullable=False, primary_key=True)
    identifier: Mapped[UniprotIdentifier] = relationship(
        "UniprotIdentifier", backref=backref("target_gene_offsets", uselist=True)
    )
    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, primary_key=True)
    target_gene: Mapped[TargetGene] = relationship(back_populates="uniprot_offset", single_parent=True)

    offset = Column(Integer, nullable=False)
