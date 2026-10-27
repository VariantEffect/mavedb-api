# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Column, Enum, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.mixins.publication_citation import PublicationCitation

if TYPE_CHECKING:
    from mavedb.models.score_calibration import ScoreCalibration


class ScoreCalibrationPublicationIdentifierAssociation(PublicationCitation, Base):
    __tablename__ = "score_calibration_publication_identifiers"

    score_calibration_id = Column(
        "score_calibration_id", Integer, ForeignKey("score_calibrations.id"), primary_key=True
    )
    relation: Mapped["ScoreCalibrationRelation"] = Column(
        Enum(ScoreCalibrationRelation, native_enum=False, validate_strings=True, length=32),
        nullable=False,
        default=ScoreCalibrationRelation.threshold,
        primary_key=True,
    )

    score_calibration: Mapped["ScoreCalibration"] = relationship(
        "mavedb.models.score_calibration.ScoreCalibration", back_populates="publication_identifier_associations"
    )
