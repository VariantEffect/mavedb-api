from datetime import date
from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship

from mavedb.db.base import Base
from mavedb.models.ensembl_identifier import EnsemblIdentifier
from mavedb.models.refseq_identifier import RefseqIdentifier
from mavedb.models.uniprot_identifier import UniprotIdentifier
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.target_accession import TargetAccession
from mavedb.models.score_set import ScoreSet

# TODO Reformat code without removing dependencies whose use is not detected.


class TargetGene(Base):
    __tablename__ = "target_genes"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String, nullable=False)
    category = Column(String, nullable=False)

    # ensembl_id_id = Column(Integer, ForeignKey('metadata_ensemblidentifier.id'), nullable=True)
    # ensembl_id_id = Column(Integer, ForeignKey('ensembl_identifiers.id'), nullable=True)
    # ensembl_id = relationship('EnsemblIdentifier', backref='target_genes')
    # refseq_id_id = Column(Integer, nullable=True)  # , ForeignKey('dataset_scoreset.id'), nullable=False)
    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), nullable=False)
    score_set : ScoreSet = relationship(
        "ScoreSet",
        backref=backref("target_genes", cascade="all,delete-orphan", single_parent=True, uselist=True),
        single_parent=True,
    )
    # uniprot_id_id = Column(Integer, nullable=True)  # , ForeignKey('dataset_scoreset.id'), nullable=False)
    target_sequence_id = Column(Integer, ForeignKey("target_sequences.id"), nullable=True)
    accession_id = Column(Integer, ForeignKey("target_accessions.id"), nullable=True)
    target_sequence : TargetSequence = relationship(
        "TargetSequence",
        backref=backref("target_genes", single_parent=True, uselist=True),
        cascade="all,delete-orphan",
        single_parent=True,
    )
    target_accession : TargetAccession = relationship(
        "TargetAccession",
        backref=backref("target_genes", single_parent=True, uselist=True),
        cascade="all,delete-orphan",
        single_parent=True,
    )

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
