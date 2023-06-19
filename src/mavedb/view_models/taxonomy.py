from datetime import date

from pydantic.types import Optional

from .base.base import BaseModel


class TaxonomyBase(BaseModel):
    tax_id: int
    organism_name: str
    common_name: str
    genome_id: Optional[int]
    creation_date: date
    modification_date: date


class TaxonomyCreate(TaxonomyBase):
    pass


class TaxonomyUpdate(TaxonomyBase):
    pass


# Properties shared by models stored in DB
class TaxonomyInDbBase(TaxonomyBase):
    id: int

    class Config:
        orm_mode = True


# Properties to return to client
class Taxonomy(TaxonomyInDbBase):
    pass


# Properties stored in DB
class TaxonomyInDbBase(TaxonomyInDbBase):
    pass