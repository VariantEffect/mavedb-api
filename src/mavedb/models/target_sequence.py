from datetime import date

from sqlalchemy import Column, Date, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base


class TargetSequence(Base):
    __tablename__ = "target_sequences"

    id = Column(Integer, primary_key=True, index=True)
    sequence_type = Column(String, nullable=False)
    sequence = Column(String, nullable=False)
    label = Column(String, nullable=True)
    taxonomy_id = Column("taxonomy_id", Integer, ForeignKey("taxonomies.id"), nullable=True)
    taxonomy = relationship(
        "Taxonomy",
        backref=backref("target_sequences", single_parent=True),
    )
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
