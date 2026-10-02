"""SQLAlchemy model for calibration controls.

A *calibration control* is a variant whose clinical significance is independently
known (from a source outside MaveDB) and which was used as empirical ground truth
when deriving a calibration's score thresholds. Storing these controls lets a
clinician audit the evidence a calibration rests on before trusting its thresholds
for interpretation.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, Date, Enum, ForeignKey, Integer, UniqueConstraint, case, select
from sqlalchemy.orm import Mapped, column_property, relationship

from mavedb.db.base import Base
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.enums.functional_classification import FunctionalClassification
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)

if TYPE_CHECKING:
    from mavedb.models.score_calibration import ScoreCalibration
    from mavedb.models.user import User
    from mavedb.models.variant import Variant


class CalibrationControl(Base):
    """A variant with independently known clinical significance, used as ground truth for a calibration.

    Distinct from ``ScoreCalibrationFunctionalClassification`` and its ``variants``
    relationship, which record *bin membership* — the range a variant's functional score
    happens to fall into. A ``CalibrationControl`` instead records *clinical ground truth*:
    "this variant's pathogenicity is known from external evidence and was used to anchor
    the calibration's thresholds." A control variant will usually also land in a bin, since
    its score should agree with its known status, but the two relationships answer different
    questions and are kept separate.

    Controls are constrained to variants in the calibration's own score set — a control
    without a functional score is meaningless as calibration evidence. That constraint is
    enforced at the API layer rather than in the schema (see issue #751).
    """

    __tablename__ = "calibration_controls"
    __table_args__ = (
        # A variant may appear at most once per calibration and cannot be double counted.
        UniqueConstraint("calibration_id", "variant_id", name="uq_calibration_controls_calibration_id_variant_id"),
    )

    id = Column(Integer, primary_key=True)

    # Deleting a calibration deletes its controls: a control has no meaning apart from the calibration it anchors.
    # The DB-level cascade backstops the ORM ``delete-orphan`` cascade declared on ``ScoreCalibration.controls``.
    calibration_id = Column(
        Integer, ForeignKey("score_calibrations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    calibration: Mapped["ScoreCalibration"] = relationship("ScoreCalibration", back_populates="controls")

    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False, index=True)
    variant: Mapped["Variant"] = relationship("Variant")

    # Which of the calibration's own functional classifications contains this control's variant by bin
    # membership, or NULL when it lands under none. Computed live from the membership association rather
    # than stored, so it can never drift from the clinical bin assignments it reports. Mirrors the
    # ``variant_count`` correlated-subquery pattern on ScoreCalibrationFunctionalClassification. Ranges may
    # overlap when one is 'not_specified' (see ScoreCalibrationBase.ranges_do_not_overlap), so a variant can
    # fall in several bins. Classified bins never overlap each other, so at most one applies; it is
    # preferred over a 'not_specified' bin (which asserts no classification), then the lowest id breaks
    # any remaining tie, for a deterministic result.
    functional_classification_id: Mapped[Optional[int]] = column_property(
        select(ScoreCalibrationFunctionalClassification.id)
        .where(
            ScoreCalibrationFunctionalClassification.calibration_id == calibration_id,
            score_calibration_functional_classification_variants_association_table.c.functional_classification_id
            == ScoreCalibrationFunctionalClassification.id,
            score_calibration_functional_classification_variants_association_table.c.variant_id == variant_id,
        )
        .order_by(
            case(
                (
                    ScoreCalibrationFunctionalClassification.functional_classification
                    == FunctionalClassification.not_specified,
                    1,
                ),
                else_=0,
            ),
            ScoreCalibrationFunctionalClassification.id,
        )
        .limit(1)
        .correlate_except(
            ScoreCalibrationFunctionalClassification,
            score_calibration_functional_classification_variants_association_table,
        )
        .scalar_subquery()
    )

    clinical_status = Column(
        Enum(CalibrationControlStatus, native_enum=False, validate_strings=True, length=32),
        nullable=False,
    )

    created_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    created_by: Mapped["User"] = relationship("User", foreign_keys="CalibrationControl.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    modified_by: Mapped["User"] = relationship("User", foreign_keys="CalibrationControl.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    def __repr__(self) -> str:  # pragma: no cover - repr utility
        return (
            f"<CalibrationControl id={self.id} calibration_id={self.calibration_id} "
            f"variant_id={self.variant_id} clinical_status={self.clinical_status}>"
        )
