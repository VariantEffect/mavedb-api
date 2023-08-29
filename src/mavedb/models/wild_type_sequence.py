from datetime import date

from sqlalchemy import Column, Date, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from .reference_genome import ReferenceGenome

from mavedb.db.base import Base


class WildTypeSequence(Base):
    __tablename__ = "wild_type_sequences"

    id = Column(Integer, primary_key=True, index=True)
    sequence_type = Column(String, nullable=False)
    sequence = Column(String, nullable=False)
    reference_id = Column("reference_id", Integer, ForeignKey("reference_genomes.id"), nullable=False)
    reference = relationship(
        "ReferenceGenome",
        backref=backref("wild_type_sequences", single_parent=True),
        single_parent=True,
    )
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
