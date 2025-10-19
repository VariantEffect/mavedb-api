"""Pydantic view models for score calibration entities.

Defines validated structures for functional score ranges, calibrations, and
associated publication/odds path references used by the API layer.
"""

from datetime import date
from typing import Any, Collection, Literal, Optional, Sequence, Union

from pydantic import field_validator, model_validator

from mavedb.lib.oddspaths import oddspaths_evidence_strength_equivalent
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_score_calibration_publication_identifiers,
    transform_score_set_to_urn,
)
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.acmg_classification import (
    ACMGClassificationBase,
    ACMGClassificationCreate,
    ACMGClassificationModify,
    SavedACMGClassification,
    ACMGClassification,
)
from mavedb.view_models.publication_identifier import (
    PublicationIdentifier,
    PublicationIdentifierBase,
    PublicationIdentifierCreate,
    SavedPublicationIdentifier,
)
from mavedb.view_models.user import SavedUser, User


### Functional range models


class FunctionalRangeBase(BaseModel):
    """Base functional range model.

    Represents a labeled numeric score interval with optional evidence metadata.
    Bounds are half-open by default (inclusive lower, exclusive upper) unless
    overridden by inclusive flags.
    """

    label: str
    description: Optional[str] = None
    classification: Literal["normal", "abnormal", "not_specified"] = "not_specified"

    range: tuple[Union[float, None], Union[float, None]]
    inclusive_lower_bound: bool = True
    inclusive_upper_bound: bool = False

    acmg_classification: Optional[ACMGClassificationBase] = None

    oddspaths_ratio: Optional[float] = None
    positive_likelihood_ratio: Optional[float] = None

    @field_validator("range")
    def ranges_are_not_backwards(
        cls, field_value: tuple[Union[float, None], Union[float, None]]
    ) -> tuple[Union[float, None], Union[float, None]]:
        """Reject reversed or zero-width intervals."""
        lower = inf_or_float(field_value[0], True)
        upper = inf_or_float(field_value[1], False)
        if lower > upper:
            raise ValidationError("The lower bound cannot exceed the upper bound.")
        if lower == upper:
            raise ValidationError("The lower and upper bounds cannot be identical.")

        return field_value

    @field_validator("oddspaths_ratio", "positive_likelihood_ratio")
    def ratios_must_be_positive(cls, field_value: Optional[float]) -> Optional[float]:
        if field_value is not None and field_value < 0:
            raise ValidationError("The ratio must be greater than or equal to 0.")

        return field_value

    @model_validator(mode="after")
    def inclusive_bounds_do_not_include_infinity(self: "FunctionalRangeBase") -> "FunctionalRangeBase":
        """Disallow inclusive bounds on unbounded (infinite) ends."""
        if self.inclusive_lower_bound and self.range[0] is None:
            raise ValidationError("An inclusive lower bound may not include negative infinity.")
        if self.inclusive_upper_bound and self.range[1] is None:
            raise ValidationError("An inclusive upper bound may not include positive infinity.")

        return self

    @model_validator(mode="after")
    def acmg_classification_evidence_agrees_with_classification(self: "FunctionalRangeBase") -> "FunctionalRangeBase":
        """If oddspaths is provided, ensure its evidence agrees with the classification."""
        if self.acmg_classification is None or self.acmg_classification.criterion is None:
            return self

        if (
            self.classification == "normal"
            and self.acmg_classification.criterion.is_pathogenic
            or self.classification == "abnormal"
            and self.acmg_classification.criterion.is_benign
        ):
            raise ValidationError(
                f"The ACMG classification criterion ({self.acmg_classification.criterion}) must agree with the functional range classification ({self.classification})."
            )

        return self

    @model_validator(mode="after")
    def oddspaths_ratio_agrees_with_acmg_classification(self: "FunctionalRangeBase") -> "FunctionalRangeBase":
        """If both oddspaths and acmg_classification are provided, ensure they agree."""
        if self.oddspaths_ratio is None or self.acmg_classification is None:
            return self

        if self.acmg_classification.criterion is None and self.acmg_classification.evidence_strength is None:
            return self

        equivalent_criterion, equivalent_strength = oddspaths_evidence_strength_equivalent(self.oddspaths_ratio)
        if (
            self.acmg_classification.criterion != equivalent_criterion
            or self.acmg_classification.evidence_strength != equivalent_strength
        ):
            raise ValidationError(
                f"The provided oddspaths_ratio ({self.oddspaths_ratio}) implies criterion {equivalent_criterion} and evidence strength {equivalent_strength},"
                f" which does not agree with the provided ACMG classification ({self.acmg_classification.criterion}, {self.acmg_classification.evidence_strength})."
            )

        return self

    def is_contained_by_range(self, score: float) -> bool:
        """Determine if a given score falls within this functional range."""
        lower_bound, upper_bound = (
            inf_or_float(self.range[0], lower=True),
            inf_or_float(self.range[1], lower=False),
        )

        lower_check = score > lower_bound or (self.inclusive_lower_bound and score == lower_bound)
        upper_check = score < upper_bound or (self.inclusive_upper_bound and score == upper_bound)

        return lower_check and upper_check


class FunctionalRangeModify(FunctionalRangeBase):
    """Model used to modify an existing functional range."""

    acmg_classification: Optional[ACMGClassificationModify] = None


class FunctionalRangeCreate(FunctionalRangeModify):
    """Model used to create a new functional range."""

    acmg_classification: Optional[ACMGClassificationCreate] = None


class SavedFunctionalRange(FunctionalRangeBase):
    """Persisted functional range model (includes record type metadata)."""

    record_type: str = None  # type: ignore
    acmg_classification: Optional[SavedACMGClassification] = None

    _record_type_factory = record_type_validator()(set_record_type)


class FunctionalRange(SavedFunctionalRange):
    """Complete functional range model returned by the API."""

    acmg_classification: Optional[ACMGClassification] = None


### Score calibration models


class ScoreCalibrationBase(BaseModel):
    """Base score calibration model.

    Provides shared fields across create, modify, saved, and full models.
    """

    title: str
    name: str
    investigator_provided: bool
    research_use_only: bool = False

    baseline_score: Optional[float] = None
    baseline_score_description: Optional[str] = None

    functional_ranges: Optional[Sequence[FunctionalRangeBase]] = None
    threshold_sources: Optional[Sequence[PublicationIdentifierBase]] = None
    classification_sources: Optional[Sequence[PublicationIdentifierBase]] = None
    method_sources: Optional[Sequence[PublicationIdentifierBase]] = None
    calibration_metadata: Optional[dict] = None

    @field_validator("functional_ranges")
    def ranges_do_not_overlap(
        cls, field_value: Optional[Sequence[FunctionalRangeBase]]
    ) -> Optional[Sequence[FunctionalRangeBase]]:
        """Ensure that no two functional ranges overlap (respecting inclusivity)."""

        def test_overlap(range_test: FunctionalRangeBase, range_check: FunctionalRangeBase) -> bool:
            if min(inf_or_float(range_test.range[0], True), inf_or_float(range_check.range[0], True)) == inf_or_float(
                range_test.range[0], True
            ):
                first, second = range_test, range_check
            else:
                first, second = range_check, range_test

            touching_and_inclusive = (
                first.inclusive_upper_bound
                and second.inclusive_lower_bound
                and inf_or_float(first.range[1], False) == inf_or_float(second.range[0], True)
            )
            if touching_and_inclusive:
                return True
            if inf_or_float(first.range[1], False) > inf_or_float(second.range[0], True):
                return True

            return False

        if not field_value:  # pragma: no cover
            return None

        for i, a in enumerate(field_value):
            for b in list(field_value)[i + 1 :]:
                if test_overlap(a, b):
                    raise ValidationError(
                        f"Score ranges may not overlap; `{a.label}` ({a.range}) overlaps with `{b.label}` ({b.range}).",
                        custom_loc=["body", "scoreCalibration", "functionalRanges", i, "range"],
                    )
        return field_value

    @model_validator(mode="after")
    def functional_range_labels_must_be_unique(self: "ScoreCalibrationBase") -> "ScoreCalibrationBase":
        """Enforce uniqueness (post-strip) of functional range labels."""
        if not self.functional_ranges:
            return self

        seen, dupes = set(), set()
        for fr in self.functional_ranges:
            fr.label = fr.label.strip()
            if fr.label in seen:
                dupes.add(fr.label)
            else:
                seen.add(fr.label)

        if dupes:
            raise ValidationError(
                f"Detected repeated label(s): {', '.join(dupes)}. Functional range labels must be unique."
            )

        return self

    @model_validator(mode="after")
    def validate_baseline_score(self: "ScoreCalibrationBase") -> "ScoreCalibrationBase":
        """If a baseline score is provided and it falls within a functional range, it may only be contained in a normal range."""
        if not self.functional_ranges:
            return self

        if self.baseline_score is None:
            return self

        for fr in self.functional_ranges:
            if fr.is_contained_by_range(self.baseline_score) and fr.classification != "normal":
                raise ValueError(
                    f"The provided baseline score of {self.baseline_score} falls within a non-normal range ({fr.label}). Baseline scores may not fall within non-normal ranges."
                )

        return self


class ScoreCalibrationModify(ScoreCalibrationBase):
    """Model used to modify an existing score calibration."""

    score_set_urn: Optional[str] = None

    functional_ranges: Optional[Sequence[FunctionalRangeModify]] = None
    threshold_sources: Optional[Sequence[PublicationIdentifierCreate]] = None
    classification_sources: Optional[Sequence[PublicationIdentifierCreate]] = None
    method_sources: Optional[Sequence[PublicationIdentifierCreate]] = None


class ScoreCalibrationCreate(ScoreCalibrationModify):
    """Model used to create a new score calibration."""

    functional_ranges: Optional[Sequence[FunctionalRangeCreate]] = None
    threshold_sources: Optional[Sequence[PublicationIdentifierCreate]] = None
    classification_sources: Optional[Sequence[PublicationIdentifierCreate]] = None
    method_sources: Optional[Sequence[PublicationIdentifierCreate]] = None


class InvestigatorProvidedScoreCalibrationCreate(ScoreCalibrationCreate):
    """Model used to create a new investigator-provided score calibration. Enforces fixed field values for certain properties."""

    investigator_provided: Literal[True] = True
    name: Literal["investigator_provided"] = "investigator_provided"


class SavedScoreCalibration(ScoreCalibrationBase):
    """Persisted score calibration model (includes identifiers and source lists)."""

    record_type: str = None  # type: ignore

    id: int
    urn: str

    score_set_id: int

    primary: bool = False
    private: bool = True

    functional_ranges: Optional[Sequence[SavedFunctionalRange]] = None
    threshold_sources: Optional[Sequence[SavedPublicationIdentifier]] = None
    classification_sources: Optional[Sequence[SavedPublicationIdentifier]] = None
    method_sources: Optional[Sequence[SavedPublicationIdentifier]] = None

    created_by: Optional[SavedUser] = None
    modified_by: Optional[SavedUser] = None
    creation_date: date
    modification_date: date

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        """Pydantic configuration (ORM mode)."""

        from_attributes = True
        arbitrary_types_allowed = True

    @field_validator("threshold_sources", "classification_sources", "method_sources", mode="before")
    def publication_identifiers_validator(cls, value: Any) -> Optional[list[PublicationIdentifier]]:
        """Coerce association proxy collections to plain lists."""
        if value is None:
            return None

        assert isinstance(value, Collection), "Publication identifier lists must be a collection"
        return list(value)

    @model_validator(mode="after")
    def primary_calibrations_may_not_be_research_use_only(self: "SavedScoreCalibration") -> "SavedScoreCalibration":
        """Primary calibrations may not be marked as research use only."""
        if self.primary and self.research_use_only:
            raise ValidationError("Primary score calibrations may not be marked as research use only.")

        return self

    @model_validator(mode="after")
    def primary_calibrations_may_not_be_private(self: "SavedScoreCalibration") -> "SavedScoreCalibration":
        """Primary calibrations may not be marked as private."""
        if self.primary and self.private:
            raise ValidationError("Primary score calibrations may not be marked as private.")

        return self

    @model_validator(mode="before")
    def generate_threshold_classification_and_method_sources(cls, data: Any):  # type: ignore[override]
        """Populate threshold/classification/method source fields from association objects if missing."""
        association_keys = {
            "threshold_sources",
            "thresholdSources",
            "classification_sources",
            "classificationSources",
            "method_sources",
            "methodSources",
        }

        if not any(hasattr(data, key) for key in association_keys):
            try:
                publication_identifiers = transform_score_calibration_publication_identifiers(
                    data.publication_identifier_associations
                )
                data.__setattr__("threshold_sources", publication_identifiers["threshold_sources"])
                data.__setattr__("classification_sources", publication_identifiers["classification_sources"])
                data.__setattr__("method_sources", publication_identifiers["method_sources"])
            except AttributeError as exc:
                raise ValidationError(
                    f"Unable to create {cls.__name__} without attribute: {exc}."  # type: ignore
                )
        return data


class ScoreCalibration(SavedScoreCalibration):
    """Complete score calibration model returned by the API."""

    functional_ranges: Optional[Sequence[FunctionalRange]] = None
    threshold_sources: Optional[Sequence[PublicationIdentifier]] = None
    classification_sources: Optional[Sequence[PublicationIdentifier]] = None
    method_sources: Optional[Sequence[PublicationIdentifier]] = None
    created_by: Optional[User] = None
    modified_by: Optional[User] = None


class ScoreCalibrationWithScoreSetUrn(SavedScoreCalibration):
    """Complete score calibration model returned by the API, with score_set_urn."""

    score_set_urn: str

    @model_validator(mode="before")
    def generate_score_set_urn(cls, data: Any):
        if not hasattr(data, "score_set_urn"):
            try:
                data.__setattr__("score_set_urn", transform_score_set_to_urn(data.score_set))
            except AttributeError as exc:
                raise ValidationError(
                    f"Unable to create {cls.__name__} without attribute: {exc}."  # type: ignore
                )
        return data
