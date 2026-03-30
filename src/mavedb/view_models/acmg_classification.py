"""Pydantic view models for ACMG-style classification and odds path entities.

Provides validated structures for ACMG criteria, evidence strengths, point-based
classifications, and associated odds path ratios.
"""

from datetime import date
from typing import Optional

from pydantic import model_validator

from mavedb.lib.acmg import (
    ACMGCriterion,
    StrengthOfEvidenceProvided,
    points_evidence_strength_equivalent,
)
from mavedb.lib.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ACMGClassificationBase(BaseModel):
    """Base ACMG classification model (criterion, evidence strength, points)."""

    criterion: Optional[ACMGCriterion] = None
    evidence_strength: Optional[StrengthOfEvidenceProvided] = None
    points: Optional[int] = None

    @model_validator(mode="after")
    def criterion_and_evidence_strength_mutually_defined(self: "ACMGClassificationBase") -> "ACMGClassificationBase":
        """Require criterion and evidence_strength to be provided together or both omitted."""
        if (self.criterion is None) != (self.evidence_strength is None):
            raise ValidationError("Both a criterion and evidence_strength must be provided together")
        return self

    @model_validator(mode="after")
    def generate_criterion_and_evidence_strength_from_points(
        self: "ACMGClassificationBase",
    ) -> "ACMGClassificationBase":
        """If points are provided but criterion and evidence_strength are not, infer them."""
        if self.points is not None and self.criterion is None and self.evidence_strength is None:
            inferred_criterion, inferred_strength = points_evidence_strength_equivalent(self.points)
            object.__setattr__(self, "criterion", inferred_criterion)
            object.__setattr__(self, "evidence_strength", inferred_strength)

        return self

    @model_validator(mode="after")
    def points_must_agree_with_evidence_strength(self: "ACMGClassificationBase") -> "ACMGClassificationBase":
        """Validate that provided points imply the same criterion and evidence strength."""
        if self.points is not None:
            inferred_criterion, inferred_strength = points_evidence_strength_equivalent(self.points)
            if (self.criterion != inferred_criterion) or (self.evidence_strength != inferred_strength):
                raise ValidationError(
                    "The provided points value does not agree with the provided criterion and evidence_strength. "
                    f"{self.points} points implies {inferred_criterion} and {inferred_strength}, but got {self.criterion} and {self.evidence_strength}."
                )

        return self


class ACMGClassificationModify(ACMGClassificationBase):
    """Model used to modify an existing ACMG classification."""

    pass


class ACMGClassificationCreate(ACMGClassificationModify):
    """Model used to create a new ACMG classification."""

    pass


class SavedACMGClassification(ACMGClassificationBase):
    """Persisted ACMG classification model (includes record type metadata)."""

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    creation_date: date
    modification_date: date

    class Config:
        """Pydantic configuration (ORM mode)."""

        from_attributes = True
        arbitrary_types_allowed = True


class ACMGClassification(SavedACMGClassification):
    """Complete ACMG classification model returned by the API."""

    pass
