# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.mixins.publication_citation import PublicationCitation

if TYPE_CHECKING:
    from mavedb.models.experiment import Experiment


class ExperimentPublicationIdentifierAssociation(PublicationCitation, Base):
    __tablename__ = "experiment_publication_identifiers"

    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    primary = Column(Boolean, nullable=False, default=False, server_default="false")

    experiment: Mapped["Experiment"] = relationship("Experiment", back_populates="publication_identifier_associations")
