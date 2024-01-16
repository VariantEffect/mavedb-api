from sqlalchemy import Column, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship, Mapped

from mavedb.db.base import Base

# Prevent circular imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mavedb.models.score_set import ScoreSet
    from mavedb.models.publication_identifier import PublicationIdentifier


class ScoreSetPublicationIdentifierAssociation(Base):
    __tablename__ = "scoreset_publication_identifiers"

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    score_set : Mapped["ScoreSet"] = relationship("mavedb.models.score_set.ScoreSet", back_populates="publication_identifier_associations")
    publication : Mapped["PublicationIdentifier"] = relationship("PublicationIdentifier")
