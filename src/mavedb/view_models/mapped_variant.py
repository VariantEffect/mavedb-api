from typing import Any

from .base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Any
    post_mapped: Any
    variant_id: int


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