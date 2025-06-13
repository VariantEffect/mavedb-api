from typing import Optional, Literal, Any, Sequence
from pydantic import validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import inf_or_float
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.publication_identifier import PublicationIdentifierBase
from mavedb.view_models.odds_path import OddsPathCreate, OddsPathBase, OddsPathModify, SavedOddsPath, OddsPath


### Range model


class ScoreRangeBase(BaseModel):
    label: str
    description: Optional[str]
    classification: Literal["normal", "abnormal", "not_specified"]
    # Purposefully vague type hint because of some odd JSON Schema generation behavior.
    # Typing this as tuple[Union[float, None], Union[float, None]] will generate an invalid
    # jsonschema, and fail all tests that access the schema. This may be fixed in pydantic v2,
    # but it's unclear. Even just typing it as Tuple[Any, Any] will generate an invalid schema!
    range: list[Any]  # really: tuple[Union[float, None], Union[float, None]]
    odds_path: Optional[OddsPathBase] = None


class ScoreRangeModify(ScoreRangeBase):
    odds_path: Optional[OddsPathModify] = None

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


class ScoreRangeCreate(ScoreRangeModify):
    odds_path: Optional[OddsPathCreate] = None


class SavedScoreRange(ScoreRangeBase):
    record_type: str = None  # type: ignore

    odds_path: Optional[SavedOddsPath] = None

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreRange(SavedScoreRange):
    odds_path: Optional[OddsPath] = None


### Ranges wrapper


class ScoreSetRangesBase(BaseModel):
    wt_score: Optional[float] = None
    ranges: Sequence[ScoreRangeBase]
    odds_path_source: Optional[Sequence[PublicationIdentifierBase]] = None


class ScoreSetRangesModify(ScoreSetRangesBase):
    ranges: Sequence[ScoreRangeModify]


class ScoreSetRangesCreate(ScoreSetRangesModify):
    ranges: Sequence[ScoreRangeCreate]


class SavedScoreSetRanges(ScoreSetRangesBase):
    record_type: str = None  # type: ignore

    ranges: Sequence[SavedScoreRange]

    _record_type_factory = record_type_validator()(set_record_type)


class ScoreSetRanges(SavedScoreSetRanges):
    ranges: Sequence[ScoreRange]
