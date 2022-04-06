from datetime import date
from typing import List

from app.view_models.experiment import SavedExperiment
from app.view_models.reference_map import ReferenceMap, ReferenceMapInDbBase
from .base.base import BaseModel


class TargetGeneBase(BaseModel):
    name: str
    category: str


class TargetGeneCreate(TargetGeneBase):
    pass


class TargetGeneUpdate(TargetGeneBase):
    pass


# Properties shared by models stored in DB
class SavedTargetGene(TargetGeneBase):
    # id: int
    reference_maps: List[ReferenceMapInDbBase]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetGene(SavedTargetGene):
    reference_maps: List[ReferenceMap]
    pass


# Properties to return to admin clients
class AdminTargetGene(SavedTargetGene):
    creation_date: date
    modification_date: date
    reference_maps: List[ReferenceMap]
    pass


# Properties stored in DB
class TargetGeneInDb(SavedTargetGene):
    experiment: SavedExperiment
