from datetime import date
from typing import Optional

from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.reference_genome import ReferenceGenome, ReferenceGenomeInDbBase


class ReferenceMapBase(BaseModel):
    # organism_name_id: int
    pass


class ReferenceMapCreate(ReferenceMapBase):
    genome_id: Optional[int]
    target_id: Optional[int]


# Properties shared by models stored in DB
class SavedReferenceMap(ReferenceMapBase):
    id: int
    genome_id: int
    target_id: int
    is_primary: bool
    genome: ReferenceGenomeInDbBase
    creation_date: date
    modification_date: date

    class Config:
        orm_mode = True


# Properties to return to client
class ReferenceMap(SavedReferenceMap):
    genome: ReferenceGenome
