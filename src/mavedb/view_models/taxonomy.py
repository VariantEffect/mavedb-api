from datetime import date

from pydantic.types import Optional

from .base.base import BaseModel


class TaxonomyBase(BaseModel):
    tax_id: int
    organism_name: Optional[str]
    common_name: Optional[str]
    rank: Optional[str]
    has_described_species_name: Optional[bool]
    article_reference: Optional[str]
    genome_id: Optional[int]


class TaxonomyCreate(TaxonomyBase):
    pass


class TaxonomyUpdate(TaxonomyBase):
    pass


# Properties shared by models stored in DB
class SavedTaxonomy(TaxonomyBase):
    id: int
    url: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class Taxonomy(SavedTaxonomy):
    pass


# Properties to return to admin clients
class AdminTaxonomy(SavedTaxonomy):
    creation_date: date
    modification_date: date