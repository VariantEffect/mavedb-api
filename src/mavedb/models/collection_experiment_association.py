"""
Association model for Collection-Experiment many-to-many relationship with ordering.
"""

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from mavedb.models.collection import Collection
    from mavedb.models.experiment import Experiment


class CollectionExperimentAssociation(Base):
    """
    Association model for the ordered many-to-many relationship between Collections and Experiments.

    The position column maintains the user-specified ordering of experiments within each collection.
    """

    __tablename__ = "collection_experiments"

    collection_id = Column(Integer, ForeignKey("collections.id"), primary_key=True)
    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    position = Column(Integer, nullable=False, default=0)

    collection: Mapped["Collection"] = relationship(
        "mavedb.models.collection.Collection",
        back_populates="experiment_associations",
    )
    experiment: Mapped["Experiment"] = relationship(
        "mavedb.models.experiment.Experiment",
    )
