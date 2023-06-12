from sqlalchemy import Column, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from mavedb.db.base import Base


class ScoreSetPublicationIdentifierAssociation(Base):
    __tablename__ = "scoreset_publication_identifiers"

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    score_set = relationship("ScoreSet", back_populates="publication_identifier_associations")
    publication = relationship("PublicationIdentifier")
