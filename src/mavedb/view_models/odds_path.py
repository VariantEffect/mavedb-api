from typing import Literal, Optional
from pydantic import validator

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class OddsPathBase(BaseModel):
    ratio: float
    evidence: Optional[
        Literal[
            "BS3_STRONG",
            "BS3_MODERATE",
            "BS3_SUPPORTING",
            "INDETERMINATE",
            "PS3_VERY_STRONG",
            "PS3_STRONG",
            "PS3_MODERATE",
            "PS3_SUPPORTING",
        ]
    ] = None


class OddsPathModify(OddsPathBase):
    @validator("ratio")
    def ratio_must_be_positive(cls, value: float) -> float:
        if value < 0:
            raise ValueError("OddsPath value must be greater than or equal to 0")

        return value


class OddsPathCreate(OddsPathModify):
    pass


class SavedOddsPath(OddsPathBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)


class OddsPath(SavedOddsPath):
    pass
