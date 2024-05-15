from datetime import date
from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship, Mapped

from typing import TYPE_CHECKING

from mavedb.db.base import Base
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.target_accession import TargetAccession
from mavedb.models.score_set import ScoreSet

if TYPE_CHECKING:
    from mavedb.models.ensembl_offset import EnsemblOffset
    from mavedb.models.refseq_offset import RefseqOffset
    from mavedb.models.uniprot_offset import UniprotOffset

# TODO Reformat code without removing dependencies whose use is not detected.


class TargetGene(Base):
    __tablename__ = "target_genes"

    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    category = Column(String, nullable=False)

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    score_set: Mapped[ScoreSet] = relationship(back_populates="target_genes", single_parent=True, uselist=True)

    target_sequence_id = Column(Integer, ForeignKey("target_sequences.id"), index=True, nullable=True)
    accession_id = Column(Integer, ForeignKey("target_accessions.id"), index=True, nullable=True)
    target_sequence: Mapped[TargetSequence] = relationship(
        back_populates="target_genes", cascade="all,delete-orphan", single_parent=True
    )
    target_accession: Mapped[TargetAccession] = relationship(
        "TargetAccession",
        backref=backref("target_genes", single_parent=True, uselist=True),
        cascade="all,delete-orphan",
        single_parent=True,
    )

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    ensembl_offset: Mapped["EnsemblOffset"] = relationship(back_populates="target_gene", cascade="all, delete-orphan")
    refseq_offset: Mapped["RefseqOffset"] = relationship(back_populates="target_gene", cascade="all, delete-orphan")
    uniprot_offset: Mapped["UniprotOffset"] = relationship(back_populates="target_gene", cascade="all, delete-orphan")
