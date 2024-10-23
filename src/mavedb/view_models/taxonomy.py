from datetime import date

from pydantic.types import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


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
    record_type: str = None  # type: ignore
    url: str

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class Taxonomy(SavedTaxonomy):
    pass


# Properties to return to admin clients
class AdminTaxonomy(SavedTaxonomy):
    creation_date: date
    modification_date: date
