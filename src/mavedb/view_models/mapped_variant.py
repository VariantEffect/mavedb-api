from typing import Any
from datetime import date

from .base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Any
    post_mapped: Any
    variant_id: int
    vrs_version: str
    error_message: str
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

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class MappedVariant(SavedMappedVariant):
    pass
