from sqlalchemy import Column, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from mavedb.db.base import Base
from mavedb.models.experiment import Experiment
from mavedb.models.publication_identifier import PublicationIdentifier


class ExperimentPublicationIdentifierAssociation(Base):
    __tablename__ = "experiment_publication_identifiers"

    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    experiment : Experiment = relationship("Experiment", back_populates="publication_identifier_associations")
    publication : PublicationIdentifier = relationship("PublicationIdentifier")
