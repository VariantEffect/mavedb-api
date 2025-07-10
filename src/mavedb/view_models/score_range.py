from typing import Any, Optional, Literal, Sequence, Union
from pydantic import validator

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
    range: list[Any]  # really: tuple[Union[float, None], Union[float, None]]

    @validator("range")
    def ranges_are_not_backwards(cls, field_value: tuple[Any]):
        if len(field_value) != 2:
            raise ValidationError("Only a lower and upper bound are allowed.")

        field_value[0] = inf_or_float(field_value[0], True) if field_value[0] is not None else None
        field_value[1] = inf_or_float(field_value[1], False) if field_value[1] is not None else None

        if inf_or_float(field_value[0], True) > inf_or_float(field_value[1], False):
            raise ValidationError("The lower bound of the score range may not be larger than the upper bound.")
        elif inf_or_float(field_value[0], True) == inf_or_float(field_value[1], False):
            raise ValidationError("The lower and upper bound of the score range may not be the same.")

        return field_value


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

    @validator("ranges")
    def ranges_do_not_overlap(cls, field_value: Sequence[ScoreRangeBase]) -> Sequence[ScoreRangeBase]:
        def test_overlap(tp1, tp2) -> bool:
            # Always check the tuple with the lowest lower bound. If we do not check
            # overlaps in this manner, checking the overlap of (0,1) and (1,2) will
            # yield different results depending on the ordering of tuples.
            if min(inf_or_float(tp1[0], True), inf_or_float(tp2[0], True)) == inf_or_float(tp1[0], True):
                tp_with_min_value = tp1
                tp_with_non_min_value = tp2
            else:
                tp_with_min_value = tp2
                tp_with_non_min_value = tp1

            if inf_or_float(tp_with_min_value[1], False) > inf_or_float(
                tp_with_non_min_value[0], True
            ) and inf_or_float(tp_with_min_value[0], True) <= inf_or_float(tp_with_non_min_value[1], False):
                return True

            return False

        for i, range_test in enumerate(field_value):
            for range_check in list(field_value)[i + 1 :]:
                if test_overlap(range_test.range, range_check.range):
                    raise ValidationError(
                        f"Score ranges may not overlap; `{range_test.label}` ({range_test.range}) overlaps with `{range_check.label}` ({range_check.range})."
                    )

        return field_value


class ScoreRangesModify(ScoreRangesBase):
    ranges: Sequence[ScoreRangeModify]


class ScoreRangesCreate(ScoreRangesModify):
    ranges: Sequence[ScoreRangeCreate]


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
    wt_score: Optional[float] = None
    ranges: Sequence[InvestigatorScoreRangeBase]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None

    @validator("wt_score")
    def score_range_normal_classification_exists_if_wild_type_score_provided(
        cls, field_value: Optional[float], values: dict[str, Any]
    ) -> Optional[float]:
        ranges = values.get("ranges", [])

        if field_value is not None:
            if not any([range_model.classification == "normal" for range_model in ranges]):
                raise ValidationError("A wild type score has been provided, but no normal classification range exists.")

        return field_value

    @validator("wt_score")
    def wild_type_score_in_normal_range(cls, field_value: Optional[float], values: dict[str, Any]) -> Optional[float]:
        ranges = values.get("ranges", [])
        wt_score = field_value

        normal_ranges = [range_model.range for range_model in ranges if range_model.classification == "normal"]

        if normal_ranges and wt_score is None:
            # For now, we do not raise an error if a normal range is provided but no wild type score.
            # raise ValidationError(
            #     "A normal range has been provided, but no wild type score has been provided.",
            #     custom_loc=["body", "scoreRanges", "wtScore"],
            # )
            return field_value

        if wt_score is None:
            return field_value

        for range in normal_ranges:
            if wt_score >= inf_or_float(range[0], lower=True) and wt_score < inf_or_float(range[1], lower=False):
                return field_value

        raise ValidationError(
            f"The provided wild type score of {wt_score} is not within any of the provided normal ranges. This score should be within a normal range.",
            custom_loc=["body", "scoreRanges", "wtScore"],
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
    # path (normal) / benign (abnormal) -> classification
    # evidence strength -> label


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
    source: Optional[Sequence[PublicationIdentifierBase]] = None


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


### Range and calibration container schemas
class SchemaContainerBase(BaseModel):
    _fields_to_exclude_for_validatation = {"record_type"}

    @validator("*")
    def score_range_labels_must_be_unique(
        cls, field_value: Union[InvestigatorScoreRangesBase, PillarProjectScoreRangesBase, None], field
    ) -> Union[InvestigatorScoreRangesBase, PillarProjectScoreRangesBase, None]:
        if field_value is None:
            return None

        # Skip validation for fields that are not score range containers
        if field.name in cls._fields_to_exclude_for_validatation:
            return field_value

        existing_labels, duplicate_labels = set(), set()
        for i, range_model in enumerate(field_value.ranges):
            range_model.label = range_model.label.strip()

            if range_model.label in existing_labels:
                duplicate_labels.add(range_model.label)
            else:
                existing_labels.add(range_model.label)

        if duplicate_labels:
            raise ValidationError(
                f"Detected repeated label(s): {', '.join(duplicate_labels)}. Range labels must be unique.",
            )

        return field_value


class RangeContainerBase(SchemaContainerBase):
    investigator_provided: Optional[InvestigatorScoreRangesBase] = None


class CalibrationContainerBase(SchemaContainerBase):
    pillar_project: Optional[PillarProjectScoreRangesBase] = None


class RangeContainerModify(RangeContainerBase):
    investigator_provided: Optional[InvestigatorScoreRangesModify] = None


class CalibrationContainerModify(CalibrationContainerBase):
    pillar_project: Optional[PillarProjectScoreRangesModify] = None


class RangeContainerCreate(RangeContainerModify):
    investigator_provided: Optional[InvestigatorScoreRangesCreate] = None


class CalibrationContainerCreate(CalibrationContainerModify):
    pillar_project: Optional[PillarProjectScoreRangesCreate] = None


class SavedRangeContainer(SchemaContainerBase):
    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)
    investigator_provided: Optional[SavedInvestigatorScoreRanges] = None


class SavedCalibrationContainer(SchemaContainerBase):
    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)
    pillar_project: Optional[SavedPillarProjectScoreRanges] = None


class RangeContainer(SavedRangeContainer):
    investigator_provided: Optional[InvestigatorScoreRanges] = None


class CalibrationContainer(SavedCalibrationContainer):
    pillar_project: Optional[PillarProjectScoreRanges] = None


### Score set range container models


class ScoreSetRangesBase(BaseModel):
    ranges: Optional[RangeContainerBase] = None
    calibrations: Optional[CalibrationContainerBase] = None


class ScoreSetRangesModify(ScoreSetRangesBase):
    ranges: Optional[RangeContainerModify] = None
    calibrations: Optional[CalibrationContainerModify] = None


class ScoreSetRangesCreate(ScoreSetRangesModify):
    ranges: Optional[RangeContainerCreate] = None
    calibrations: Optional[CalibrationContainerCreate] = None


class SavedScoreSetRanges(ScoreSetRangesBase):
    record_type: str = None  # type: ignore

    ranges: Optional[SavedRangeContainer] = None
    calibrations: Optional[SavedCalibrationContainer] = None

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreSetRanges(SavedScoreSetRanges):
    ranges: Optional[RangeContainer] = None
    calibrations: Optional[CalibrationContainer] = None
