from datetime import date
from typing import TYPE_CHECKING, List

from sqlalchemy import Column, Date, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base

from .taxonomy import Taxonomy

if TYPE_CHECKING:
    from mavedb.models.score_set import ScoreSet
    from mavedb.models.target_gene import TargetGene


class TargetSequence(Base):
    __tablename__ = "target_sequences"
    # The target gene's foreign key includes score_set_id, so the key carried here can't drift from it (#809).
    __table_args__ = (UniqueConstraint("id", "score_set_id", name="uq_target_sequences_id_score_set_id"),)

    id = Column(Integer, primary_key=True)
    sequence_type = Column(String, nullable=False)
    sequence = Column(String, nullable=False)
    label = Column(String, nullable=True)
    taxonomy_id = Column("taxonomy_id", Integer, ForeignKey("taxonomies.id"), index=True, nullable=True)
    taxonomy: Mapped[Taxonomy] = relationship(
        "Taxonomy",
        backref=backref("target_sequences", single_parent=True),
    )
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    # The score set of the target gene that owns this sequence, carried so the RLS policy can check it without
    # reading target_genes: the api inserts a sequence before any target gene points at it (#809). Set from the
    # target gene when the sequence is created (``mavedb.models.target_gene``).
    score_set_id = Column(Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    score_set: Mapped["ScoreSet"] = relationship("ScoreSet")

    target_genes: Mapped[List["TargetGene"]] = relationship(
        back_populates="target_sequence",
        primaryjoin="TargetGene.target_sequence_id == TargetSequence.id",
        foreign_keys="TargetGene.target_sequence_id",
    )
