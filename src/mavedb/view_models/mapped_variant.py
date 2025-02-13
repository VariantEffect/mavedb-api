from datetime import date
from typing import Any, Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Optional[Any] = None
    post_mapped: Optional[Any] = None
    variant_id: int
    vrs_version: Optional[str] = None
    error_message: Optional[str] = None
    modification_date: date
    mapped_date: date
    mapping_api_version: str
    current: bool


class MappedVariantCreate(MappedVariantBase):
    pass


class MappedVariantUpdate(MappedVariantBase):
    pass


# Properties shared by models stored in DB
class SavedMappedVariant(MappedVariantBase):
    id: int
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to non-admin clients
class MappedVariant(SavedMappedVariant):
    pass
