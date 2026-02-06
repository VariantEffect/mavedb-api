"""Pydantic view models for score calibration entities.

Defines validated structures for functional score ranges, calibrations, and
associated publication/odds path references used by the API layer.
"""

from datetime import date
from typing import TYPE_CHECKING, Any, Collection, Optional, Sequence, Union

from pydantic import Field, field_validator, model_validator

from mavedb.lib.oddspaths import oddspaths_evidence_strength_equivalent
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_score_calibration_publication_identifiers,
    transform_score_set_to_urn,
)
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassifcationOptions
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.acmg_classification import (
    ACMGClassification,
    ACMGClassificationBase,
    ACMGClassificationCreate,
    ACMGClassificationModify,
    SavedACMGClassification,
)
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.publication_identifier import (
    PublicationIdentifier,
    PublicationIdentifierBase,
    PublicationIdentifierCreate,
    SavedPublicationIdentifier,
)
from mavedb.view_models.user import SavedUser, User

if TYPE_CHECKING:
    from mavedb.view_models.variant import (
        SavedVariantEffectMeasurement,
        VariantEffectMeasurement,
    )

### Functional range models


class FunctionalClassificationBase(BaseModel):
    """Base functional range model.

    Represents a labeled numeric score interval with optional evidence metadata.
    Bounds are half-open by default (inclusive lower, exclusive upper) unless
    overridden by inclusive flags.
    """

    label: str
    description: Optional[str] = None
    functional_classification: FunctionalClassifcationOptions = FunctionalClassifcationOptions.not_specified

    range: Optional[tuple[Union[float, None], Union[float, None]]] = None  # (lower_bound, upper_bound)
    class_: Optional[str] = Field(None, alias="class", serialization_alias="class")

    inclusive_lower_bound: Optional[bool] = None
    inclusive_upper_bound: Optional[bool] = None

    acmg_classification: Optional[ACMGClassificationBase] = None

    oddspaths_ratio: Optional[float] = None
    positive_likelihood_ratio: Optional[float] = None

    class Config:
        populate_by_name = True

    @field_validator("range")
    def ranges_are_not_backwards(
        cls, field_value: Optional[tuple[Union[float, None], Union[float, None]]]
    ) -> Optional[tuple[Union[float, None], Union[float, None]]]:
        """Reject reversed or zero-width intervals."""
        if field_value is None:
            return None

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

    @field_validator("class_", "label", mode="before")
    def labels_and_class_strip_whitespace_and_validate_not_empty(cls, field_value: Optional[str]) -> Optional[str]:
        """Strip leading/trailing whitespace from class names."""
        if field_value is None:
            return None

        field_value = field_value.strip()
        if not field_value:
            raise ValidationError("This field may not be empty or contain only whitespace.")

        return field_value

    @model_validator(mode="after")
    def At_least_one_of_range_or_class_must_be_provided(
        self: "FunctionalClassificationBase",
    ) -> "FunctionalClassificationBase":
        """Either a range or a class must be provided."""
        if self.range is None and self.class_ is None:
            raise ValidationError("A functional range must specify either a numeric range or a class.")

        return self

    @model_validator(mode="after")
    def class_and_range_mutually_exclusive(
        self: "FunctionalClassificationBase",
    ) -> "FunctionalClassificationBase":
        """Either a range or a class may be provided, but not both."""
        if self.range is not None and self.class_ is not None:
            raise ValidationError("A functional range may not specify both a numeric range and a class.")

        return self

    @model_validator(mode="after")
    def inclusive_bounds_require_range(self: "FunctionalClassificationBase") -> "FunctionalClassificationBase":
        """Inclusive bounds may only be set if a range is provided. If they are unset, default them."""
        if self.class_ is not None:
            if self.inclusive_lower_bound is not None:
                raise ValidationError(
                    "An inclusive lower bound may not be set on a class based functional classification."
                )
            if self.inclusive_upper_bound is not None:
                raise ValidationError(
                    "An inclusive upper bound may not be set on a class based functional classification."
                )

        if self.range is not None:
            if self.inclusive_lower_bound is None:
                self.inclusive_lower_bound = True
            if self.inclusive_upper_bound is None:
                self.inclusive_upper_bound = False

        return self

    @model_validator(mode="after")
    def inclusive_bounds_do_not_include_infinity(
        self: "FunctionalClassificationBase",
    ) -> "FunctionalClassificationBase":
        """Disallow inclusive bounds on unbounded (infinite) ends."""
        if self.inclusive_lower_bound and self.range is not None and self.range[0] is None:
            raise ValidationError("An inclusive lower bound may not include negative infinity.")
        if self.inclusive_upper_bound and self.range is not None and self.range[1] is None:
            raise ValidationError("An inclusive upper bound may not include positive infinity.")

        return self

    @model_validator(mode="after")
    def acmg_classification_evidence_agrees_with_classification(
        self: "FunctionalClassificationBase",
    ) -> "FunctionalClassificationBase":
        """If oddspaths is provided, ensure its evidence agrees with the classification."""
        if self.acmg_classification is None or self.acmg_classification.criterion is None:
            return self

        if (
            self.functional_classification is FunctionalClassifcationOptions.normal
            and self.acmg_classification.criterion.is_pathogenic
            or self.functional_classification is FunctionalClassifcationOptions.abnormal
            and self.acmg_classification.criterion.is_benign
        ):
            raise ValidationError(
                f"The ACMG classification criterion ({self.acmg_classification.criterion}) must agree with the functional range classification ({self.functional_classification})."
            )

        return self

    @model_validator(mode="after")
    def oddspaths_ratio_agrees_with_acmg_classification(
        self: "FunctionalClassificationBase",
    ) -> "FunctionalClassificationBase":
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
        if not self.range:
            return False

        lower_bound, upper_bound = (
            inf_or_float(self.range[0], lower=True),
            inf_or_float(self.range[1], lower=False),
        )

        lower_check = score > lower_bound or (self.inclusive_lower_bound is True and score == lower_bound)
        upper_check = score < upper_bound or (self.inclusive_upper_bound is True and score == upper_bound)

        return lower_check and upper_check

    @property
    def class_based(self) -> bool:
        """Determine if this functional classification is class-based."""
        return self.class_ is not None

    @property
    def range_based(self) -> bool:
        """Determine if this functional classification is range-based."""
        return self.range is not None


class FunctionalClassificationModify(FunctionalClassificationBase):
    """Model used to modify an existing functional range."""

    acmg_classification: Optional[ACMGClassificationModify] = None


class FunctionalClassificationCreate(FunctionalClassificationModify):
    """Model used to create a new functional range."""

    acmg_classification: Optional[ACMGClassificationCreate] = None


class SavedFunctionalClassification(FunctionalClassificationBase):
    """Persisted functional range model (includes record type metadata)."""

    record_type: str = None  # type: ignore
    acmg_classification: Optional[SavedACMGClassification] = None
    variants: Sequence["SavedVariantEffectMeasurement"] = []

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        """Pydantic configuration (ORM mode)."""

        from_attributes = True
        arbitrary_types_allowed = True


class FunctionalClassification(SavedFunctionalClassification):
    """Complete functional range model returned by the API."""

    acmg_classification: Optional[ACMGClassification] = None
    variants: Sequence["VariantEffectMeasurement"] = []


### Score calibration models


class ScoreCalibrationBase(BaseModel):
    """Base score calibration model.

    Provides shared fields across create, modify, saved, and full models.
    """

    title: str
    research_use_only: bool = False

    baseline_score: Optional[float] = None
    baseline_score_description: Optional[str] = None
    notes: Optional[str] = None

    functional_classifications: Optional[Sequence[FunctionalClassificationBase]] = None
    threshold_sources: Sequence[PublicationIdentifierBase]
    classification_sources: Sequence[PublicationIdentifierBase]
    method_sources: Sequence[PublicationIdentifierBase]
    calibration_metadata: Optional[dict] = None

    @field_validator("functional_classifications")
    def ranges_do_not_overlap(
        cls, field_value: Optional[Sequence[FunctionalClassificationBase]]
    ) -> Optional[Sequence[FunctionalClassificationBase]]:
        """Ensure that no two functional ranges overlap (respecting inclusivity)."""

        def test_overlap(range_test: FunctionalClassificationBase, range_check: FunctionalClassificationBase) -> bool:
            # Allow 'not_specified' classifications to overlap with anything.
            if (
                range_test.functional_classification is FunctionalClassifcationOptions.not_specified
                or range_check.functional_classification is FunctionalClassifcationOptions.not_specified
                or range_test.range is None
                or range_check.range is None
            ):
                return False

            if min(inf_or_float(range_test.range[0], True), inf_or_float(range_check.range[0], True)) == inf_or_float(
                range_test.range[0], True
            ):
                first, second = range_test, range_check
            else:
                first, second = range_check, range_test

            # The range types below that mypy complains about are verified by the earlier checks for None.
            touching_and_inclusive = (
                first.inclusive_upper_bound
                and second.inclusive_lower_bound
                and inf_or_float(first.range[1], False) == inf_or_float(second.range[0], True)  # type: ignore
            )
            if touching_and_inclusive:
                return True
            if inf_or_float(first.range[1], False) > inf_or_float(second.range[0], True):  # type: ignore
                return True

            return False

        if not field_value:  # pragma: no cover
            return None

        for i, a in enumerate(field_value):
            for b in list(field_value)[i + 1 :]:
                if test_overlap(a, b):
                    raise ValidationError(
                        f"Classified score ranges may not overlap; `{a.label}` ({a.range}) overlaps with `{b.label}` ({b.range}). To allow overlap, set one or both classifications to 'not_specified'.",
                        custom_loc=["body", i, "range"],
                    )
        return field_value

    @model_validator(mode="after")
    def functional_range_labels_classes_must_be_unique(self: "ScoreCalibrationBase") -> "ScoreCalibrationBase":
        """Enforce uniqueness (post-strip) of functional range labels and classes."""
        if not self.functional_classifications:
            return self

        seen_l, dupes_l = set(), set()
        seen_c, dupes_c = set(), set()
        for i, fr in enumerate(self.functional_classifications):
            if fr.label in seen_l:
                dupes_l.add((fr.label, i))
            else:
                seen_l.add(fr.label)

            if fr.class_ is not None:
                if fr.class_ in seen_c:
                    dupes_c.add((fr.class_, i))
                else:
                    seen_c.add(fr.class_)

        if dupes_l:
            raise ValidationError(
                f"Detected repeated label(s): {', '.join(label for label, _ in dupes_l)}. Functional range labels must be unique.",
                custom_loc=["body", "functionalClassifications", dupes_l.pop()[1], "label"],
            )
        if dupes_c:
            raise ValidationError(
                f"Detected repeated class name(s): {', '.join(class_name for class_name, _ in dupes_c)}. Functional range class names must be unique.",
                custom_loc=["body", "functionalClassifications", dupes_c.pop()[1], "class"],
            )

        return self

    @model_validator(mode="after")
    def validate_baseline_score(self: "ScoreCalibrationBase") -> "ScoreCalibrationBase":
        """If a baseline score is provided and it falls within a functional range, it may only be contained in a normal range."""
        if not self.functional_classifications:
            return self

        if self.baseline_score is None:
            return self

        for fr in self.functional_classifications:
            if (
                fr.is_contained_by_range(self.baseline_score)
                and fr.functional_classification is not FunctionalClassifcationOptions.normal
            ):
                raise ValidationError(
                    f"The provided baseline score of {self.baseline_score} falls within a non-normal range ({fr.label}). Baseline scores may not fall within non-normal ranges.",
                    custom_loc=["body", "baselineScore"],
                )

        return self

    @model_validator(mode="after")
    def functional_classifications_must_be_of_same_type(
        self: "ScoreCalibrationBase",
    ) -> "ScoreCalibrationBase":
        """All functional classifications must be either range-based or class-based."""
        if not self.functional_classifications:
            return self

        range_based_count = sum(1 for fc in self.functional_classifications if fc.range_based)
        class_based_count = sum(1 for fc in self.functional_classifications if fc.class_based)

        if range_based_count > 0 and class_based_count > 0:
            raise ValidationError(
                "All functional classifications within a score calibration must be of the same type (either all range-based or all class-based).",
                custom_loc=["body", "functionalClassifications"],
            )

        return self

    @property
    def range_based(self) -> bool:
        """Determine if this score calibration is range-based."""
        if not self.functional_classifications:
            return False

        return self.functional_classifications[0].range_based

    @property
    def class_based(self) -> bool:
        """Determine if this score calibration is class-based."""
        if not self.functional_classifications:
            return False

        return self.functional_classifications[0].class_based


class ScoreCalibrationModify(ScoreCalibrationBase):
    """Model used to modify an existing score calibration."""

    score_set_urn: Optional[str] = None

    functional_classifications: Optional[Sequence[FunctionalClassificationModify]] = None
    threshold_sources: Sequence[PublicationIdentifierCreate]
    classification_sources: Sequence[PublicationIdentifierCreate]
    method_sources: Sequence[PublicationIdentifierCreate]


class ScoreCalibrationCreate(ScoreCalibrationModify):
    """Model used to create a new score calibration."""

    functional_classifications: Optional[Sequence[FunctionalClassificationCreate]] = None
    threshold_sources: Sequence[PublicationIdentifierCreate]
    classification_sources: Sequence[PublicationIdentifierCreate]
    method_sources: Sequence[PublicationIdentifierCreate]


class SavedScoreCalibration(ScoreCalibrationBase):
    """Persisted score calibration model (includes identifiers and source lists)."""

    record_type: str = None  # type: ignore

    id: int
    urn: str

    score_set_id: int

    investigator_provided: bool
    primary: bool = False
    private: bool = True

    functional_classifications: Optional[Sequence[SavedFunctionalClassification]] = None
    threshold_sources: Sequence[SavedPublicationIdentifier]
    classification_sources: Sequence[SavedPublicationIdentifier]
    method_sources: Sequence[SavedPublicationIdentifier]

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
        assert isinstance(value, Collection), "Publication identifier lists must be a collection"
        return list(value)

    @model_validator(mode="after")
    def primary_calibrations_may_not_be_research_use_only(self: "SavedScoreCalibration") -> "SavedScoreCalibration":
        """Primary calibrations may not be marked as research use only."""
        if self.primary and self.research_use_only:
            raise ValidationError(
                "Primary score calibrations may not be marked as research use only.",
                custom_loc=["body", "researchUseOnly"],
            )

        return self

    @model_validator(mode="after")
    def primary_calibrations_may_not_be_private(self: "SavedScoreCalibration") -> "SavedScoreCalibration":
        """Primary calibrations may not be marked as private."""
        if self.primary and self.private:
            raise ValidationError(
                "Primary score calibrations may not be marked as private.", custom_loc=["body", "private"]
            )

        return self

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created. Only perform these
    # transformations if the relevant attributes are present on the input data (i.e., when creating from an ORM object).
    @model_validator(mode="before")
    def generate_threshold_classification_and_method_sources(cls, data: Any):  # type: ignore[override]
        """Populate threshold/classification/method source fields from association objects if missing."""
        if hasattr(data, "publication_identifier_associations"):
            try:
                publication_identifiers = transform_score_calibration_publication_identifiers(
                    data.publication_identifier_associations
                )
                data.__setattr__("threshold_sources", publication_identifiers["threshold_sources"])
                data.__setattr__("classification_sources", publication_identifiers["classification_sources"])
                data.__setattr__("method_sources", publication_identifiers["method_sources"])
            except (AttributeError, KeyError) as exc:
                raise ValidationError(
                    f"Unable to coerce publication associations for {cls.__name__}: {exc}."  # type: ignore
                )
        return data


class ScoreCalibration(SavedScoreCalibration):
    """Complete score calibration model returned by the API."""

    functional_classifications: Optional[Sequence[FunctionalClassification]] = None
    threshold_sources: Sequence[PublicationIdentifier]
    classification_sources: Sequence[PublicationIdentifier]
    method_sources: Sequence[PublicationIdentifier]
    created_by: Optional[User] = None
    modified_by: Optional[User] = None


class ScoreCalibrationWithScoreSetUrn(SavedScoreCalibration):
    """Complete score calibration model returned by the API, with score_set_urn."""

    score_set_urn: str

    @model_validator(mode="before")
    def generate_score_set_urn(cls, data: Any):
        if hasattr(data, "score_set"):
            try:
                data.__setattr__("score_set_urn", transform_score_set_to_urn(data.score_set))
            except (AttributeError, KeyError) as exc:
                raise ValidationError(
                    f"Unable to coerce score set urn for {cls.__name__}: {exc}."  # type: ignore
                )
        return data
