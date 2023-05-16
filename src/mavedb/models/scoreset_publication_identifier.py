from sqlalchemy import Column, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from mavedb.db.base import Base


class ScoresetPublicationIdentifierAssociation(Base):
    __tablename__ = "scoreset_publication_identifiers"

    scoreset_id = Column(Integer, ForeignKey("scoresets.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    scoreset = relationship("Scoreset", back_populates="publication_identifier_associations")
    publication = relationship("PublicationIdentifier")
