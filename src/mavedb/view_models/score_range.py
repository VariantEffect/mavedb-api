import operator
from typing import Optional, Literal, Sequence, Union
from pydantic import field_validator, model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.publication_identifier import PublicationIdentifierBase
from mavedb.view_models.odds_path import OddsPathCreate, OddsPathBase, OddsPathModify, SavedOddsPath, OddsPath


##############################################################################################################
# Base score range models. To be inherited by other score range models.
##############################################################################################################


### Base range models


class ScoreRangeBase(BaseModel):
    label: str
    description: Optional[str] = None
    classification: Literal["normal", "abnormal", "not_specified"] = "not_specified"
    # Purposefully vague type hint because of some odd JSON Schema generation behavior.
    # Typing this as tuple[Union[float, None], Union[float, None]] will generate an invalid
    # jsonschema, and fail all tests that access the schema. This may be fixed in pydantic v2,
    # but it's unclear. Even just typing it as Tuple[Any, Any] will generate an invalid schema!
    range: tuple[Union[float, None], Union[float, None]]
    inclusive_lower_bound: bool = True
    inclusive_upper_bound: bool = False

    @field_validator("range")
    def ranges_are_not_backwards(
        cls, field_value: tuple[Union[float, None], Union[float, None]]
    ) -> tuple[Union[float, None], Union[float, None]]:
        lower = inf_or_float(field_value[0], True)
        upper = inf_or_float(field_value[1], False)

        if lower > upper:
            raise ValidationError("The lower bound of the score range may not be larger than the upper bound.")
        elif lower == upper:
            raise ValidationError("The lower and upper bound of the score range may not be the same.")

        return field_value

    # @root_validator
    @model_validator(mode="after")
    def inclusive_bounds_do_not_include_infinity(self: "ScoreRangeBase") -> "ScoreRangeBase":
        """
        Ensure that if the lower bound is inclusive, it does not include negative infinity.
        Similarly, if the upper bound is inclusive, it does not include positive infinity.
        """
        range_values = self.range
        inclusive_lower_bound = self.inclusive_lower_bound
        inclusive_upper_bound = self.inclusive_upper_bound

        if inclusive_lower_bound and range_values[0] is None:
            raise ValidationError("An inclusive lower bound may not include negative infinity.")
        if inclusive_upper_bound and range_values[1] is None:
            raise ValidationError("An inclusive upper bound may not include positive infinity.")

        return self


class ScoreRangeModify(ScoreRangeBase):
    pass


class ScoreRangeCreate(ScoreRangeModify):
    pass


class SavedScoreRange(ScoreRangeBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreRange(SavedScoreRange):
    pass


### Base wrapper models


class ScoreRangesBase(BaseModel):
    ranges: Sequence[ScoreRangeBase]
    source: Optional[Sequence[PublicationIdentifierBase]] = None

    @field_validator("ranges")
    def ranges_do_not_overlap(cls, field_value: Sequence[ScoreRangeBase]) -> Sequence[ScoreRangeBase]:
        def test_overlap(range_test: ScoreRangeBase, range_check: ScoreRangeBase) -> bool:
            # Always check the tuple with the lowest lower bound. If we do not check
            # overlaps in this manner, checking the overlap of (0,1) and (1,2) will
            # yield different results depending on the ordering of tuples.
            if min(inf_or_float(range_test.range[0], True), inf_or_float(range_check.range[0], True)) == inf_or_float(
                range_test.range[0], True
            ):
                range_with_min_value = range_test
                range_with_non_min_value = range_check
            else:
                range_with_min_value = range_check
                range_with_non_min_value = range_test

            adjacent_boundary_comparator = (
                operator.gt
                if range_with_min_value.inclusive_upper_bound or range_with_non_min_value.inclusive_lower_bound
                else operator.ge
            )

            # If both ranges have inclusive bounds and their bounds intersect, we consider them overlapping.
            if (
                range_with_min_value.inclusive_upper_bound
                and range_with_non_min_value.inclusive_lower_bound
                and (
                    inf_or_float(range_with_min_value.range[1], False)
                    == inf_or_float(range_with_non_min_value.range[0], True)
                )
            ):
                return True

            # Since we have ordered the ranges, it's a guarantee that the lower bound of the first range is less
            # than or equal to the lower bound of the second range. If the upper bound of the first range is greater
            # than or equal to the lower bound of the second range, then the two ranges overlap. Note that if either
            # of these ranges has an inclusive upper or lower bound, we should compare them without the equality operator.
            if adjacent_boundary_comparator(
                inf_or_float(range_with_min_value.range[1], False),
                inf_or_float(range_with_non_min_value.range[0], True),
            ):
                return True

            return False

        for i, range_test in enumerate(field_value):
            for range_check in list(field_value)[i + 1 :]:
                if test_overlap(range_test, range_check):
                    raise ValidationError(
                        f"Score ranges may not overlap; `{range_test.label}` ({range_test.range}) overlaps with `{range_check.label}` ({range_check.range})."
                    )

        return field_value


class ScoreRangesModify(ScoreRangesBase):
    ranges: Sequence[ScoreRangeModify]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None


class ScoreRangesCreate(ScoreRangesModify):
    ranges: Sequence[ScoreRangeCreate]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None


class SavedScoreRanges(ScoreRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedScoreRange]

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreRanges(SavedScoreRanges):
    ranges: Sequence[ScoreRange]


##############################################################################################################
# Investigator provided score range models
##############################################################################################################

### Investigator provided score range model


class InvestigatorScoreRangeBase(ScoreRangeBase):
    odds_path: Optional[OddsPathBase] = None


class InvestigatorScoreRangeModify(ScoreRangeModify, InvestigatorScoreRangeBase):
    odds_path: Optional[OddsPathModify] = None


class InvestigatorScoreRangeCreate(ScoreRangeCreate, InvestigatorScoreRangeModify):
    odds_path: Optional[OddsPathCreate] = None


class SavedInvestigatorScoreRange(SavedScoreRange, InvestigatorScoreRangeBase):
    record_type: str = None  # type: ignore

    odds_path: Optional[SavedOddsPath] = None

    _record_type_factory = record_type_validator()(set_record_type)


class InvestigatorScoreRange(ScoreRange, SavedInvestigatorScoreRange):
    odds_path: Optional[OddsPath] = None


### Investigator provided score range wrapper model


class InvestigatorScoreRangesBase(ScoreRangesBase):
    baseline_score: Optional[float] = None
    baseline_score_description: Optional[str] = None
    ranges: Sequence[InvestigatorScoreRangeBase]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None

    @model_validator(mode="after")
    def validate_baseline_score(self: "InvestigatorScoreRangesBase") -> "InvestigatorScoreRangesBase":
        ranges = getattr(self, "ranges", []) or []
        baseline_score = getattr(self, "baseline_score", None)

        if baseline_score is not None:
            if not any(range_model.classification == "normal" for range_model in ranges):
                raise ValidationError("A baseline score has been provided, but no normal classification range exists.")

        normal_ranges = [range_model.range for range_model in ranges if range_model.classification == "normal"]

        if normal_ranges and baseline_score is None:
            # For now, we do not raise an error if a normal range is provided but no baseline score.
            return self

        if baseline_score is None:
            return self

        for r in normal_ranges:
            if baseline_score >= inf_or_float(r[0], lower=True) and baseline_score < inf_or_float(r[1], lower=False):
                return self

        raise ValidationError(
            f"The provided baseline score of {baseline_score} is not within any of the provided normal ranges. This score should be within a normal range.",
            custom_loc=["body", "scoreRanges", "baselineScore"],
        )


class InvestigatorScoreRangesModify(ScoreRangesModify, InvestigatorScoreRangesBase):
    ranges: Sequence[InvestigatorScoreRangeModify]


class InvestigatorScoreRangesCreate(ScoreRangesCreate, InvestigatorScoreRangesModify):
    ranges: Sequence[InvestigatorScoreRangeCreate]


class SavedInvestigatorScoreRanges(SavedScoreRanges, InvestigatorScoreRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedInvestigatorScoreRange]

    _record_type_factory = record_type_validator()(set_record_type)


class InvestigatorScoreRanges(ScoreRanges, SavedInvestigatorScoreRanges):
    ranges: Sequence[InvestigatorScoreRange]


##############################################################################################################
# Pillar project specific calibration models
##############################################################################################################

### Pillar project score range model


class PillarProjectScoreRangeBase(ScoreRangeBase):
    positive_likelihood_ratio: Optional[float] = None
    evidence_strength: int
    # path (normal) / benign (abnormal) -> classification

    @model_validator(mode="after")
    def evidence_strength_cardinality_must_agree_with_classification(self: "PillarProjectScoreRangeBase") -> "PillarProjectScoreRangeBase":
        classification = getattr(self, "classification")
        field_value = getattr(self, "evidence_strength")

        if classification == "normal" and field_value >= 0:
            raise ValidationError(
                "The evidence strength for a normal range must be negative.",
            )
        elif classification == "abnormal" and field_value <= 0:
            raise ValidationError(
                "The evidence strength for an abnormal range must be positive.",
            )

        return self


class PillarProjectScoreRangeModify(ScoreRangeModify, PillarProjectScoreRangeBase):
    pass


class PillarProjectScoreRangeCreate(ScoreRangeCreate, PillarProjectScoreRangeModify):
    pass


class SavedPillarProjectScoreRange(SavedScoreRange, PillarProjectScoreRangeBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)


class PillarProjectScoreRange(ScoreRange, SavedPillarProjectScoreRange):
    pass


### Pillar project score range wrapper model


class PillarProjectParameters(BaseModel):
    skew: float
    location: float
    scale: float


class PillarProjectParameterSet(BaseModel):
    functionally_altering: PillarProjectParameters
    functionally_normal: PillarProjectParameters
    fraction_functionally_altering: float


class PillarProjectScoreRangesBase(ScoreRangesBase):
    prior_probability_pathogenicity: Optional[float] = None
    parameter_sets: list[PillarProjectParameterSet] = []
    ranges: Sequence[PillarProjectScoreRangeBase]


class PillarProjectScoreRangesModify(ScoreRangesModify, PillarProjectScoreRangesBase):
    ranges: Sequence[PillarProjectScoreRangeModify]


class PillarProjectScoreRangesCreate(ScoreRangesCreate, PillarProjectScoreRangesModify):
    ranges: Sequence[PillarProjectScoreRangeCreate]


class SavedPillarProjectScoreRanges(SavedScoreRanges, PillarProjectScoreRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedPillarProjectScoreRange]

    _record_type_factory = record_type_validator()(set_record_type)


class PillarProjectScoreRanges(ScoreRanges, SavedPillarProjectScoreRanges):
    ranges: Sequence[PillarProjectScoreRange]


###############################################################################################################
# Score range container objects
###############################################################################################################

### Score set range container models


class ScoreSetRangesBase(BaseModel):
    investigator_provided: Optional[InvestigatorScoreRangesBase] = None
    pillar_project: Optional[PillarProjectScoreRangesBase] = None

    _fields_to_exclude_for_validatation = {"record_type"}

    @model_validator(mode="after")
    def score_range_labels_must_be_unique(self: "ScoreSetRangesBase") -> "ScoreSetRangesBase":
        for container in (self.investigator_provided, self.pillar_project):
            if container is None:
                continue

            existing_labels, duplicate_labels = set(), set()
            for range_model in container.ranges:
                range_model.label = range_model.label.strip()
                if range_model.label in existing_labels:
                    duplicate_labels.add(range_model.label)
                else:
                    existing_labels.add(range_model.label)

            if duplicate_labels:
                raise ValidationError(
                    f"Detected repeated label(s): {', '.join(duplicate_labels)}. Range labels must be unique.",
                )
        return self


class ScoreSetRangesModify(ScoreSetRangesBase):
    investigator_provided: Optional[InvestigatorScoreRangesModify] = None
    pillar_project: Optional[PillarProjectScoreRangesModify] = None


class ScoreSetRangesCreate(ScoreSetRangesModify):
    investigator_provided: Optional[InvestigatorScoreRangesCreate] = None
    pillar_project: Optional[PillarProjectScoreRangesCreate] = None


class SavedScoreSetRanges(ScoreSetRangesBase):
    record_type: str = None  # type: ignore

    investigator_provided: Optional[SavedInvestigatorScoreRanges] = None
    pillar_project: Optional[SavedPillarProjectScoreRanges] = None

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreSetRanges(SavedScoreSetRanges):
    investigator_provided: Optional[InvestigatorScoreRanges] = None
    pillar_project: Optional[PillarProjectScoreRanges] = None
