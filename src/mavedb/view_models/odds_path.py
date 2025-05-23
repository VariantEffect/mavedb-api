from typing import Literal, Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.publication_identifier import PublicationIdentifierBase


class OddsPathRatio(BaseModel):
    normal: float
    abnormal: float


class OddsPathEvidenceStrengths(BaseModel):
    normal: Literal["BS3_STRONG"]
    abnormal: Literal["PS3_STRONG"]


class OddsPathBase(BaseModel):
    ratios: OddsPathRatio
    evidence_strengths: OddsPathEvidenceStrengths


class OddsPathModify(OddsPathBase):
    source: Optional[list[PublicationIdentifierBase]] = None


class OddsPathCreate(OddsPathModify):
    pass


class SavedOddsPath(OddsPathBase):
    record_type: str = None  # type: ignore

    source: Optional[Sequence[PublicationIdentifierBase]] = None

    _record_type_factory = record_type_validator()(set_record_type)


class OddsPath(SavedOddsPath):
    pass
