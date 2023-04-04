from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base


class RefseqOffset(Base):
    __tablename__ = "refseq_offsets"

    identifier_id = Column(Integer, ForeignKey("refseq_identifiers.id"), nullable=False, primary_key=True)
    identifier = relationship("RefseqIdentifier", backref=backref("target_gene_offsets", uselist=True))
    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, primary_key=True)
    target_gene = relationship(
        "TargetGene",
        backref=backref("refseq_offset", cascade="all,delete-orphan", single_parent=True, uselist=False),
        single_parent=True,
    )
    offset = Column(Integer, nullable=False)
