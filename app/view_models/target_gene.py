from datetime import date
from typing import Any, List, Optional

from pydantic import conlist
from pydantic.utils import GetterDict

from app.view_models import external_gene_identifier_offset
from app.view_models.base.base import BaseModel
from app.view_models.reference_map import ReferenceMap, ReferenceMapCreate
from app.view_models.wild_type_sequence import WildTypeSequence, WildTypeSequenceCreate


class ExternalIdentifiersGetter(GetterDict):
    def get(self, key: str, default: Any) -> Any:
        if key == 'external_identifiers':
            ensembl_offset = getattr(self._obj, 'ensembl_offset')
            refseq_offset = getattr(self._obj, 'refseq_offset')
            uniprot_offset = getattr(self._obj, 'uniprot_offset')
            return list(filter(lambda x: x is not None, [ensembl_offset, refseq_offset, uniprot_offset]))
        else:
            return super().get(key, default)


class TargetGeneBase(BaseModel):
    name: str
    category: str
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetBase]


class TargetGeneCreate(TargetGeneBase):
    reference_maps: conlist(ReferenceMapCreate, min_items=1)
    wt_sequence: WildTypeSequenceCreate
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


class TargetGeneUpdate(TargetGeneBase):
    pass
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


# Properties shared by models stored in DB
class SavedTargetGene(TargetGeneBase):
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetBase]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = ExternalIdentifiersGetter


# Properties to return to non-admin clients
class TargetGene(SavedTargetGene):
    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter


# Properties to return in a list context
class ShortTargetGene(SavedTargetGene):
    reference_maps: List[ReferenceMap]


# Properties to return to admin clients
class AdminTargetGene(SavedTargetGene):
    creation_date: date
    modification_date: date
    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter
