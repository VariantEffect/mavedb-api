from datetime import date
from typing import Any

from pydantic.types import Optional

from .base.base import BaseModel


class VariantBase(BaseModel):
    urn: Optional[str]
    data: Any
    score_set_id: int
    hgvs_nt: Optional[str]
    hgvs_pro: Optional[str]
    hgvs_splice: Optional[str]
    creation_date: date
    modification_date: date


class VariantCreate(VariantBase):
    pass


class VariantUpdate(VariantBase):
    pass


# Properties shared by models stored in DB
class VariantInDbBase(VariantBase):
    id: int

    class Config:
        orm_mode = True


# Properties to return to client
class Variant(VariantInDbBase):
    pass


# Properties stored in DB
class VariantInDb(VariantInDbBase):
    pass
