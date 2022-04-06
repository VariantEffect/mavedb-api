from datetime import date

from app.view_models.reference_genome import ReferenceGenome, ReferenceGenomeInDbBase
from .base.base import BaseModel


class ReferenceMapBase(BaseModel):
    is_primary: bool
    genome_id: int
    target_id: int
    # organism_name_id: int
    creation_date: date
    modification_date: date


class ReferenceMapCreate(ReferenceMapBase):
    pass


class ReferenceMapUpdate(ReferenceMapBase):
    pass


# Properties shared by models stored in DB
class ReferenceMapInDbBase(ReferenceMapBase):
    id: int
    genome: ReferenceGenomeInDbBase

    class Config:
        orm_mode = True


# Properties to return to client
class ReferenceMap(ReferenceMapInDbBase):
    genome: ReferenceGenome


# Properties stored in DB
class ReferenceMapInDb(ReferenceMapInDbBase):
    pass
