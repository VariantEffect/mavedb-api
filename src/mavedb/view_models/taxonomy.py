from datetime import date
from typing import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class TaxonomyBase(BaseModel):
    code: int
    organism_name: Optional[str] = None
    common_name: Optional[str] = None
    rank: Optional[str] = None
    has_described_species_name: Optional[bool] = None
    article_reference: Optional[str] = None
    genome_id: Optional[int] = None


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
        from_attributes = True


# Properties to return to non-admin clients
class Taxonomy(SavedTaxonomy):
    pass


# Properties to return to admin clients
class AdminTaxonomy(SavedTaxonomy):
    creation_date: date
    modification_date: date
