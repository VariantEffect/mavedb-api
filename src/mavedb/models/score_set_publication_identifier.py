# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from mavedb.models.publication_identifier import PublicationIdentifier
    from mavedb.models.score_set import ScoreSet


class ScoreSetPublicationIdentifierAssociation(Base):
    __tablename__ = "scoreset_publication_identifiers"

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), primary_key=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    primary = Column(Boolean, nullable=True, default=False)

    score_set: Mapped["ScoreSet"] = relationship(
        "mavedb.models.score_set.ScoreSet", back_populates="publication_identifier_associations"
    )
    publication: Mapped["PublicationIdentifier"] = relationship("PublicationIdentifier")
