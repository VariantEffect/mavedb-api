from datetime import date

from pydantic.types import Optional

from .base.base import BaseModel


class ReferenceGenomeBase(BaseModel):
    short_name: str
    organism_name: str
    genome_id: Optional[int]
    creation_date: date
    modification_date: date


class ReferenceGenomeCreate(ReferenceGenomeBase):
    pass


class ReferenceGenomeUpdate(ReferenceGenomeBase):
    pass


# Properties shared by models stored in DB
class ReferenceGenomeInDbBase(ReferenceGenomeBase):
    id: int

    class Config:
        orm_mode = True


# Properties to return to client
class ReferenceGenome(ReferenceGenomeInDbBase):
    pass


# Properties stored in DB
class ReferenceGenomeInDb(ReferenceGenomeInDbBase):
    pass
