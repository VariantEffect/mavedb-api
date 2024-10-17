from datetime import date
from typing import TYPE_CHECKING, List

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base

from .taxonomy import Taxonomy

if TYPE_CHECKING:
    from mavedb.models.target_gene import TargetGene


class TargetSequence(Base):
    __tablename__ = "target_sequences"

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

    target_genes: Mapped[List["TargetGene"]] = relationship(back_populates="target_sequence")
