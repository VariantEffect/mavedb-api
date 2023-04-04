from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.target_gene import TargetGene


class EnsemblOffset(Base):
    __tablename__ = "ensembl_offsets"

    identifier_id = Column(Integer, ForeignKey("ensembl_identifiers.id"), nullable=False, primary_key=True)
    identifier = relationship("EnsemblIdentifier", backref=backref("target_gene_offsets", uselist=True))
    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, primary_key=True)
    target_gene = relationship(
        "TargetGene",
        backref=backref("ensembl_offset", cascade="all,delete-orphan", single_parent=True, uselist=False),
        single_parent=True,
    )
    offset = Column(Integer, nullable=False)
