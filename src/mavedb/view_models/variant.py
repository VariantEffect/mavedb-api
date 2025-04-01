from datetime import date
from typing import Any, Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class VariantBase(BaseModel):
    urn: Optional[str] = None
    data: Any
    score_set_id: int
    hgvs_nt: Optional[str] = None
    hgvs_pro: Optional[str] = None
    hgvs_splice: Optional[str] = None
    creation_date: date
    modification_date: date


class VariantCreate(VariantBase):
    pass


class VariantUpdate(VariantBase):
    pass


# Properties shared by models stored in DB
class SavedVariant(VariantBase):
    id: int
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to client
class Variant(SavedVariant):
    pass
