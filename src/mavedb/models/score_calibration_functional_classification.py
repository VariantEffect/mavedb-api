"""SQLAlchemy model for variant score calibration functional classifications."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)

if TYPE_CHECKING:
    from mavedb.models.score_calibration import ScoreCalibration
    from mavedb.models.variant import Variant


class ScoreCalibrationFunctionalClassification(Base):
    __tablename__ = "score_calibration_functional_classifications"

    id = Column(Integer, primary_key=True)

    calibration_id = Column(Integer, ForeignKey("score_calibrations.id"), nullable=False)
    calibration: Mapped["ScoreCalibration"] = relationship("ScoreCalibration", foreign_keys=[calibration_id])

    label = Column(String, nullable=False)
    description = Column(String, nullable=True)

    functional_classification = Column(
        Enum(FunctionalClassificationOptions, native_enum=False, validate_strings=True, length=32),
        nullable=False,
        default=FunctionalClassificationOptions.not_specified,
    )

    range = Column(JSONB(none_as_null=True), nullable=True)  # (lower_bound, upper_bound)
    class_ = Column(String, nullable=True)

    inclusive_lower_bound = Column(Boolean, nullable=True)
    inclusive_upper_bound = Column(Boolean, nullable=True)

    oddspaths_ratio = Column(Float, nullable=True)
    positive_likelihood_ratio = Column(Float, nullable=True)

    acmg_classification_id = Column(Integer, ForeignKey("acmg_classifications.id"), nullable=True)
    acmg_classification: Mapped[ACMGClassification] = relationship(
        "ACMGClassification", foreign_keys=[acmg_classification_id]
    )

    # Many-to-many relationship with variants
    variants: Mapped[list["Variant"]] = relationship(
        "Variant",
        secondary=score_calibration_functional_classification_variants_association_table,
    )

    def score_is_contained_in_range(self, score: float) -> bool:
        """Check if a given score falls within the defined range."""
        if self.range is None or not isinstance(self.range, list) or len(self.range) != 2:
            return False

        lower_bound, upper_bound = inf_or_float(self.range[0], lower=True), inf_or_float(self.range[1], lower=False)
        if self.inclusive_lower_bound:
            if score < lower_bound:
                return False
        else:
            if score <= lower_bound:
                return False

        if self.inclusive_upper_bound:
            if score > upper_bound:
                return False
        else:
            if score >= upper_bound:
                return False

        return True
