from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from mavedb.models.score_set import ScoreSet


class TargetAccession(Base):
    __tablename__ = "target_accessions"
    # The target gene's foreign key includes score_set_id, so the key carried here can't drift from it (#809).
    __table_args__ = (UniqueConstraint("id", "score_set_id", name="uq_target_accessions_id_score_set_id"),)

    id = Column(Integer, primary_key=True)
    assembly = Column(String, nullable=True)
    accession = Column(String, nullable=False)
    gene = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    is_base_editor = Column(Boolean, nullable=False, default=False)

    # The score set of the target gene that owns this accession, carried so the RLS policy can check it without
    # reading target_genes: the api inserts an accession before any target gene points at it (#809). Set from the
    # target gene when the accession is created (``mavedb.models.target_gene``).
    score_set_id = Column(Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    score_set: Mapped["ScoreSet"] = relationship("ScoreSet")
