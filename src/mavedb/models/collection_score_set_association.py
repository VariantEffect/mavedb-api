"""
Association model for Collection-ScoreSet many-to-many relationship with ordering.
"""

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from mavedb.models.collection import Collection
    from mavedb.models.score_set import ScoreSet


class CollectionScoreSetAssociation(Base):
    """
    Association model for the ordered many-to-many relationship between Collections and ScoreSets.

    The position column maintains the user-specified ordering of score sets within each collection.
    """

    __tablename__ = "collection_score_sets"

    collection_id = Column(Integer, ForeignKey("collections.id"), primary_key=True)
    score_set_id = Column(Integer, ForeignKey("scoresets.id"), primary_key=True)
    position = Column(Integer, nullable=False, default=0)

    collection: Mapped["Collection"] = relationship(
        "mavedb.models.collection.Collection",
        back_populates="score_set_associations",
    )
    score_set: Mapped["ScoreSet"] = relationship(
        "mavedb.models.score_set.ScoreSet",
    )
