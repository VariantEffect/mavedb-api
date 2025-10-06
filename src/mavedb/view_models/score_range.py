from typing import Optional, Literal, Sequence, Union
from pydantic import field_validator, model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.publication_identifier import (
    PublicationIdentifierBase,
    PublicationIdentifierCreate,
)
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
    title: str
    research_use_only: bool
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
            # than the lower bound of the second range, then the two ranges overlap. Inclusive bounds only come into
            # play when the boundaries are equal and both bounds are inclusive.
            if inf_or_float(range_with_min_value.range[1], False) > inf_or_float(
                range_with_non_min_value.range[0], True
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
    source: Optional[Sequence[PublicationIdentifierCreate]] = None


class ScoreRangesCreate(ScoreRangesModify):
    ranges: Sequence[ScoreRangeCreate]


class ScoreRangesAdminCreate(ScoreRangesCreate):
    primary: bool = False


class SavedScoreRanges(ScoreRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedScoreRange]
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreRanges(SavedScoreRanges):
    ranges: Sequence[ScoreRange]


##############################################################################################################
# Brnich style score range models
##############################################################################################################


class BrnichScoreRangeBase(ScoreRangeBase):
    odds_path: Optional[OddsPathBase] = None


class BrnichScoreRangeModify(ScoreRangeModify, BrnichScoreRangeBase):
    odds_path: Optional[OddsPathModify] = None


class BrnichScoreRangeCreate(ScoreRangeCreate, BrnichScoreRangeModify):
    odds_path: Optional[OddsPathCreate] = None


class SavedBrnichScoreRange(SavedScoreRange, BrnichScoreRangeBase):
    record_type: str = None  # type: ignore

    odds_path: Optional[SavedOddsPath] = None

    _record_type_factory = record_type_validator()(set_record_type)


class BrnichScoreRange(ScoreRange, SavedBrnichScoreRange):
    odds_path: Optional[OddsPath] = None


### Brnich score range wrapper model


class BrnichScoreRangesBase(ScoreRangesBase):
    baseline_score: Optional[float] = None
    baseline_score_description: Optional[str] = None
    ranges: Sequence[BrnichScoreRangeBase]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None

    @model_validator(mode="after")
    def validate_baseline_score(self: "BrnichScoreRangesBase") -> "BrnichScoreRangesBase":
        ranges = getattr(self, "ranges", []) or []
        baseline_score = getattr(self, "baseline_score", None)

        if baseline_score is not None:
            if not any(range_model.classification == "normal" for range_model in ranges):
                # For now, we do not raise an error if a baseline score is provided but no normal range exists.
                # raise ValidationError("A baseline score has been provided, but no normal classification range exists.")
                return self

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


class BrnichScoreRangesModify(ScoreRangesModify, BrnichScoreRangesBase):
    ranges: Sequence[BrnichScoreRangeModify]
    odds_path_source: Optional[Sequence[PublicationIdentifierCreate]] = None


class BrnichScoreRangesCreate(ScoreRangesCreate, BrnichScoreRangesModify):
    ranges: Sequence[BrnichScoreRangeCreate]


class BrnichScoreRangesAdminCreate(ScoreRangesAdminCreate, BrnichScoreRangesCreate):
    pass


class SavedBrnichScoreRanges(SavedScoreRanges, BrnichScoreRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedBrnichScoreRange]
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class BrnichScoreRanges(ScoreRanges, SavedBrnichScoreRanges):
    ranges: Sequence[BrnichScoreRange]


##############################################################################################################
# Investigator provided score range models
##############################################################################################################


# NOTE: Pydantic takes the first occurence of a field definition in the MRO for default values. It feels most
# natural to define these classes like
# class InvestigatorScoreRangesBase(BrnichScoreRangesBase):
#    title: str = "Investigator-provided functional classes"
#
# class InvestigatorScoreRangesModify(BrnichScoreRangesModify, InvestigatorScoreRangesBase):
#    pass
#
# however, this does not work because the title field is defined in BrnichScoreRangesBase, and the default
# value from that class is taken instead of the one in InvestigatorScoreRangesBase. Note the opposite problem
# would occur if we defined the classes in the opposite order.
#
# We'd also like to retain the inheritance chain from Base -> Modify -> Create and Base -> Saved -> Full for
# each score range type as this makes it much easier to use these classes in inherited types from other
# modules (like the ScoreSet models). So although a mixin class might seem natural, we can't use one here
# since our MRO resolution wouldn't be linear.
#
# Just duplicating the defaults across each of the classes is the simplest solution for now, despite the
# code duplication.


class InvestigatorScoreRangesBase(BrnichScoreRangesBase):
    title: str = "Investigator-provided functional classes"
    research_use_only: bool = False


class InvestigatorScoreRangesModify(BrnichScoreRangesModify, InvestigatorScoreRangesBase):
    title: str = "Investigator-provided functional classes"
    research_use_only: bool = False


class InvestigatorScoreRangesCreate(BrnichScoreRangesCreate, InvestigatorScoreRangesModify):
    title: str = "Investigator-provided functional classes"
    research_use_only: bool = False


class InvestigatorScoreRangesAdminCreate(ScoreRangesAdminCreate, InvestigatorScoreRangesCreate):
    pass


class SavedInvestigatorScoreRanges(SavedBrnichScoreRanges, InvestigatorScoreRangesBase):
    record_type: str = None  # type: ignore

    title: str = "Investigator-provided functional classes"
    research_use_only: bool = False
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class InvestigatorScoreRanges(BrnichScoreRanges, SavedInvestigatorScoreRanges):
    title: str = "Investigator-provided functional classes"
    research_use_only: bool = False


##############################################################################################################
# Scott score range models
##############################################################################################################


class ScottScoreRangesBase(BrnichScoreRangesBase):
    title: str = "Scott calibration"
    research_use_only: bool = False


class ScottScoreRangesModify(BrnichScoreRangesModify, ScottScoreRangesBase):
    title: str = "Scott calibration"
    research_use_only: bool = False


class ScottScoreRangesCreate(BrnichScoreRangesCreate, ScottScoreRangesModify):
    title: str = "Scott calibration"
    research_use_only: bool = False


class ScottScoreRangesAdminCreate(ScoreRangesAdminCreate, ScottScoreRangesCreate):
    pass


class SavedScottScoreRanges(SavedBrnichScoreRanges, ScottScoreRangesBase):
    record_type: str = None  # type: ignore

    title: str = "Scott calibration"
    research_use_only: bool = False
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class ScottScoreRanges(BrnichScoreRanges, SavedScottScoreRanges):
    title: str = "Scott calibration"
    research_use_only: bool = False


##############################################################################################################
# IGVF Coding Variant Focus Group (CVFG) range models
##############################################################################################################

# Controls: All Variants


class IGVFCodingVariantFocusGroupControlScoreRangesBase(BrnichScoreRangesBase):
    title: str = "IGVF Coding Variant Focus Group -- Controls: All Variants"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupControlScoreRangesModify(
    BrnichScoreRangesModify, IGVFCodingVariantFocusGroupControlScoreRangesBase
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: All Variants"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupControlScoreRangesCreate(
    BrnichScoreRangesCreate, IGVFCodingVariantFocusGroupControlScoreRangesModify
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: All Variants"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate(
    ScoreRangesAdminCreate, IGVFCodingVariantFocusGroupControlScoreRangesCreate
):
    pass


class SavedIGVFCodingVariantFocusGroupControlScoreRanges(
    SavedBrnichScoreRanges, IGVFCodingVariantFocusGroupControlScoreRangesBase
):
    record_type: str = None  # type: ignore

    title: str = "IGVF Coding Variant Focus Group -- Controls: All Variants"
    research_use_only: bool = False
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class IGVFCodingVariantFocusGroupControlScoreRanges(
    BrnichScoreRanges, SavedIGVFCodingVariantFocusGroupControlScoreRanges
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: All Variants"
    research_use_only: bool = False


# Controls: Missense Variants


class IGVFCodingVariantFocusGroupMissenseScoreRangesBase(BrnichScoreRangesBase):
    title: str = "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupMissenseScoreRangesModify(
    BrnichScoreRangesModify, IGVFCodingVariantFocusGroupMissenseScoreRangesBase
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupMissenseScoreRangesCreate(
    BrnichScoreRangesCreate, IGVFCodingVariantFocusGroupMissenseScoreRangesModify
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only"
    research_use_only: bool = False


class IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate(
    ScoreRangesAdminCreate, IGVFCodingVariantFocusGroupMissenseScoreRangesCreate
):
    pass


class SavedIGVFCodingVariantFocusGroupMissenseScoreRanges(
    SavedBrnichScoreRanges, IGVFCodingVariantFocusGroupMissenseScoreRangesBase
):
    record_type: str = None  # type: ignore

    title: str = "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only"
    research_use_only: bool = False
    primary: bool = False

    _record_type_factory = record_type_validator()(set_record_type)


class IGVFCodingVariantFocusGroupMissenseScoreRanges(
    BrnichScoreRanges, SavedIGVFCodingVariantFocusGroupMissenseScoreRanges
):
    title: str = "IGVF Coding Variant Focus Group -- Controls: Missense Variants Only"
    research_use_only: bool = False


##############################################################################################################
# Zeiberg specific calibration models
##############################################################################################################

### Zeiberg score range model


class ZeibergCalibrationScoreRangeBase(ScoreRangeBase):
    positive_likelihood_ratio: Optional[float] = None
    evidence_strength: int
    # path (normal) / benign (abnormal) -> classification

    @model_validator(mode="after")
    def evidence_strength_cardinality_must_agree_with_classification(
        self: "ZeibergCalibrationScoreRangeBase",
    ) -> "ZeibergCalibrationScoreRangeBase":
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


class ZeibergCalibrationScoreRangeModify(ScoreRangeModify, ZeibergCalibrationScoreRangeBase):
    pass


class ZeibergCalibrationScoreRangeCreate(ScoreRangeCreate, ZeibergCalibrationScoreRangeModify):
    pass


class SavedZeibergCalibrationScoreRange(SavedScoreRange, ZeibergCalibrationScoreRangeBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)


class ZeibergCalibrationScoreRange(ScoreRange, SavedZeibergCalibrationScoreRange):
    pass


### Zeiberg score range wrapper model


class ZeibergCalibrationParameters(BaseModel):
    skew: float
    location: float
    scale: float


class ZeibergCalibrationParameterSet(BaseModel):
    functionally_altering: ZeibergCalibrationParameters
    functionally_normal: ZeibergCalibrationParameters
    fraction_functionally_altering: float


class ZeibergCalibrationScoreRangesBase(ScoreRangesBase):
    title: str = "Zeiberg calibration"
    research_use_only: bool = True

    prior_probability_pathogenicity: Optional[float] = None
    parameter_sets: list[ZeibergCalibrationParameterSet] = []
    ranges: Sequence[ZeibergCalibrationScoreRangeBase]


class ZeibergCalibrationScoreRangesModify(ScoreRangesModify, ZeibergCalibrationScoreRangesBase):
    title: str = "Zeiberg calibration"
    research_use_only: bool = True
    ranges: Sequence[ZeibergCalibrationScoreRangeModify]


class ZeibergCalibrationScoreRangesCreate(ScoreRangesCreate, ZeibergCalibrationScoreRangesModify):
    title: str = "Zeiberg calibration"
    research_use_only: bool = True
    ranges: Sequence[ZeibergCalibrationScoreRangeCreate]


class ZeibergCalibrationScoreRangesAdminCreate(ScoreRangesAdminCreate, ZeibergCalibrationScoreRangesCreate):
    pass


class SavedZeibergCalibrationScoreRanges(SavedScoreRanges, ZeibergCalibrationScoreRangesBase):
    record_type: str = None  # type: ignore

    title: str = "Zeiberg calibration"
    research_use_only: bool = True
    primary: bool = False
    ranges: Sequence[SavedZeibergCalibrationScoreRange]

    _record_type_factory = record_type_validator()(set_record_type)


class ZeibergCalibrationScoreRanges(ScoreRanges, SavedZeibergCalibrationScoreRanges):
    title: str = "Zeiberg calibration"
    research_use_only: bool = True
    ranges: Sequence[ZeibergCalibrationScoreRange]


###############################################################################################################
# Score range container objects
###############################################################################################################

### Score set range container models

# TODO#518: Generic score range keys for supported calibration formats.


class ScoreSetRangesBase(BaseModel):
    investigator_provided: Optional[InvestigatorScoreRangesBase] = None
    scott_calibration: Optional[ScottScoreRangesBase] = None
    zeiberg_calibration: Optional[ZeibergCalibrationScoreRangesBase] = None
    cvfg_all_variants: Optional[IGVFCodingVariantFocusGroupControlScoreRangesBase] = None
    cvfg_missense_variants: Optional[IGVFCodingVariantFocusGroupMissenseScoreRangesBase] = None

    _fields_to_exclude_for_validatation = {"record_type"}

    @model_validator(mode="after")
    def score_range_labels_must_be_unique(self: "ScoreSetRangesBase") -> "ScoreSetRangesBase":
        for container in (
            self.investigator_provided,
            self.zeiberg_calibration,
            self.scott_calibration,
            self.cvfg_all_variants,
            self.cvfg_missense_variants,
        ):
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
    scott_calibration: Optional[ScottScoreRangesModify] = None
    zeiberg_calibration: Optional[ZeibergCalibrationScoreRangesModify] = None
    cvfg_all_variants: Optional[IGVFCodingVariantFocusGroupControlScoreRangesModify] = None
    cvfg_missense_variants: Optional[IGVFCodingVariantFocusGroupMissenseScoreRangesModify] = None


class ScoreSetRangesCreate(ScoreSetRangesModify):
    investigator_provided: Optional[InvestigatorScoreRangesCreate] = None
    scott_calibration: Optional[ScottScoreRangesCreate] = None
    zeiberg_calibration: Optional[ZeibergCalibrationScoreRangesCreate] = None
    cvfg_all_variants: Optional[IGVFCodingVariantFocusGroupControlScoreRangesCreate] = None
    cvfg_missense_variants: Optional[IGVFCodingVariantFocusGroupMissenseScoreRangesCreate] = None


class SavedScoreSetRanges(ScoreSetRangesBase):
    record_type: str = None  # type: ignore

    investigator_provided: Optional[SavedInvestigatorScoreRanges] = None
    scott_calibration: Optional[SavedScottScoreRanges] = None
    zeiberg_calibration: Optional[SavedZeibergCalibrationScoreRanges] = None
    cvfg_all_variants: Optional[SavedIGVFCodingVariantFocusGroupControlScoreRanges] = None
    cvfg_missense_variants: Optional[SavedIGVFCodingVariantFocusGroupMissenseScoreRanges] = None

    _record_type_factory = record_type_validator()(set_record_type)

    @model_validator(mode="after")
    def one_and_only_one_primary_score_range_set(self: "SavedScoreSetRanges") -> "SavedScoreSetRanges":
        primary_count = 0
        for container in (
            self.investigator_provided,
            self.zeiberg_calibration,
            self.scott_calibration,
            self.cvfg_all_variants,
            self.cvfg_missense_variants,
        ):
            if container is None:
                continue
            if getattr(container, "primary", False):
                primary_count += 1

        # Set the investigator provided score ranges as primary if no other primary is set.
        if primary_count == 0 and self.investigator_provided is not None:
            self.investigator_provided.primary = True

        elif primary_count > 1:
            raise ValidationError(
                f"A maximum of one score range set must be marked as primary, but {primary_count} were.",
                custom_loc=["body", "scoreRanges"],
            )

        return self


class ScoreSetRanges(SavedScoreSetRanges):
    investigator_provided: Optional[InvestigatorScoreRanges] = None
    scott_calibration: Optional[ScottScoreRanges] = None
    zeiberg_calibration: Optional[ZeibergCalibrationScoreRanges] = None
    cvfg_all_variants: Optional[IGVFCodingVariantFocusGroupControlScoreRanges] = None
    cvfg_missense_variants: Optional[IGVFCodingVariantFocusGroupMissenseScoreRanges] = None
