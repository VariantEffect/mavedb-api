from datetime import date
from typing import List, Optional

from pydantic import conlist

from app.view_models.base.base import BaseModel
from app.view_models.reference_map import ReferenceMap, ReferenceMapCreate
from app.view_models.wild_type_sequence import WildTypeSequence, WildTypeSequenceCreate


class TargetGeneBase(BaseModel):
    name: str
    category: str


class TargetGeneCreate(TargetGeneBase):
    ensembl_id_id: Optional[int]
    refseq_id_id: Optional[int]
    uniprot_id_id: Optional[int]
    reference_maps: conlist(ReferenceMapCreate, min_items=1)
    wt_sequence: WildTypeSequenceCreate


class TargetGeneUpdate(TargetGeneBase):
    pass


# Properties shared by models stored in DB
class SavedTargetGene(TargetGeneBase):
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetGene(SavedTargetGene):
    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
    pass


# Properties to return in a list context
class ShortTargetGene(SavedTargetGene):
    reference_maps: List[ReferenceMap]
    pass


# Properties to return to admin clients
class AdminTargetGene(SavedTargetGene):
    creation_date: date
    modification_date: date
    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
