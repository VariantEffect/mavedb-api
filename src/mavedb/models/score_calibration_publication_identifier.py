# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, Enum
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation

if TYPE_CHECKING:
    from mavedb.models.publication_identifier import PublicationIdentifier
    from mavedb.models.score_calibration import ScoreCalibration


class ScoreCalibrationPublicationIdentifierAssociation(Base):
    __tablename__ = "score_calibration_publication_identifiers"

    score_calibration_id = Column(
        "score_calibration_id", Integer, ForeignKey("score_calibrations.id"), primary_key=True
    )
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), primary_key=True)
    relation: Mapped["ScoreCalibrationRelation"] = Column(
        Enum(ScoreCalibrationRelation, native_enum=False, validate_strings=True, length=32),
        nullable=False,
        default=ScoreCalibrationRelation.threshold,
        primary_key=True,
    )

    score_calibration: Mapped["ScoreCalibration"] = relationship(
        "mavedb.models.score_calibration.ScoreCalibration", back_populates="publication_identifier_associations"
    )
    publication: Mapped["PublicationIdentifier"] = relationship("PublicationIdentifier")
