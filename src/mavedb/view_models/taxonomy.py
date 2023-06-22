from datetime import date

from pydantic.types import Optional

from .base.base import BaseModel, validator

class TaxonomyBase(BaseModel):
    tax_id: int
    organism_name: Optional[str]
    common_name: Optional[str]
    rank: Optional[str]
    has_described_species_name: Optional[bool]
    article_reference: str
    genome_id: Optional[int]
    creation_date: date
    modification_date: date


class TaxonomyCreate(TaxonomyBase):
    pass
    """
    # For creating a new Taxonomy if we haven't added it in database but it's valued.
    @validator("tax_id")
    def must_be_valid_tax_id(cls, v):
        if not idutils.is_pmid(v):
            raise ValueError("{} is not a valid taxonomy ID.".format(v))
        return v
    """
class TaxonomyUpdate(TaxonomyBase):
    pass


# Properties shared by models stored in DB
class TaxonomyInDbBase(TaxonomyBase):
    id: int
    url: str

    class Config:
        orm_mode = True


# Properties to return to client
class Taxonomy(TaxonomyInDbBase):
    pass


# Properties stored in DB
class TaxonomyInDbBase(TaxonomyInDbBase):
    pass