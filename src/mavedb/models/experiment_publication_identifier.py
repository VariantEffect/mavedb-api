# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from mavedb.models.experiment import Experiment
    from mavedb.models.publication_identifier import PublicationIdentifier


class ExperimentPublicationIdentifierAssociation(Base):
    __tablename__ = "experiment_publication_identifiers"

    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    experiment: Mapped["Experiment"] = relationship("Experiment", back_populates="publication_identifier_associations")
    publication: Mapped["PublicationIdentifier"] = relationship("PublicationIdentifier")
