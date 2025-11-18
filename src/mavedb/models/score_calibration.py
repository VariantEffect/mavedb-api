"""SQLAlchemy model for variant score calibrations."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.lib.urns import generate_calibration_urn
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_publication_identifier import ScoreCalibrationPublicationIdentifierAssociation

if TYPE_CHECKING:
    from mavedb.models.publication_identifier import PublicationIdentifier
    from mavedb.models.score_set import ScoreSet
    from mavedb.models.user import User


class ScoreCalibration(Base):
    __tablename__ = "score_calibrations"
    # TODO#544: Add a partial unique index to enforce only one primary calibration per score set.

    id = Column(Integer, primary_key=True)
    urn = Column(String(64), nullable=True, default=generate_calibration_urn, unique=True, index=True)

    score_set_id = Column(Integer, ForeignKey("scoresets.id"), nullable=False)
    score_set: Mapped["ScoreSet"] = relationship("ScoreSet", back_populates="score_calibrations")

    title = Column(String, nullable=False)
    research_use_only = Column(Boolean, nullable=False, default=False)
    primary = Column(Boolean, nullable=False, default=False)
    investigator_provided: Mapped[bool] = Column(Boolean, nullable=False, default=False)
    private = Column(Boolean, nullable=False, default=True)
    notes = Column(String, nullable=True)

    baseline_score = Column(Float, nullable=True)
    baseline_score_description = Column(String, nullable=True)

    # Ranges and sources are stored as JSONB (intersection structure) to avoid complex joins for now.
    # ranges: list[ { label, description?, classification, range:[lower,upper], inclusive_lower_bound, inclusive_upper_bound } ]
    functional_ranges_deprecated_json = Column(JSONB(none_as_null=True), nullable=True)
    functional_ranges: Mapped[list["ScoreCalibrationFunctionalClassification"]] = relationship(
        "ScoreCalibrationFunctionalClassification",
        back_populates="calibration",
        cascade="all, delete-orphan",
    )

    publication_identifier_associations: Mapped[list[ScoreCalibrationPublicationIdentifierAssociation]] = relationship(
        "ScoreCalibrationPublicationIdentifierAssociation",
        back_populates="score_calibration",
        cascade="all, delete-orphan",
    )
    publication_identifiers: AssociationProxy[list[PublicationIdentifier]] = association_proxy(
        "publication_identifier_associations",
        "publication",
        creator=lambda p: ScoreCalibrationPublicationIdentifierAssociation(publication=p, relation=p.relation),
    )

    calibration_metadata = Column(JSONB(none_as_null=True), nullable=True)

    created_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    created_by: Mapped["User"] = relationship("User", foreign_keys="ScoreCalibration.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    modified_by: Mapped["User"] = relationship("User", foreign_keys="ScoreCalibration.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    def __repr__(self) -> str:  # pragma: no cover - repr utility
        return (
            f"<ScoreCalibration id={self.id} score_set_id={self.score_set_id} "
            f"title={self.title!r} primary={self.primary}>"
        )
