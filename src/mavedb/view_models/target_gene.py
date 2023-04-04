from datetime import date
from typing import Any, List

from pydantic import conlist
from pydantic.utils import GetterDict

from mavedb.view_models import external_gene_identifier_offset
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.reference_map import ReferenceMap, ReferenceMapCreate
from mavedb.view_models.wild_type_sequence import WildTypeSequence, WildTypeSequenceCreate
from mavedb.lib.validation import target


class ExternalIdentifiersGetter(GetterDict):
    """
    Custom class used in transforming TargetGene SQLAlchemy model objects into Pydantic view model objects, with special
    handling of external identifiers for target genes.

    Pydantic uses GetterDict objects to access source objects as dictionaries, which can then be turned into Pydantic
    view model objects. Here we need to remap the underlying SQLAlchemy model's separate properties for Ensembl, RefSeq,
    and UniProt identifiers into an array of external identifiers.
    """

    def get(self, key: str, default: Any) -> Any:
        if key == "external_identifiers":
            ensembl_offset = getattr(self._obj, "ensembl_offset")
            refseq_offset = getattr(self._obj, "refseq_offset")
            uniprot_offset = getattr(self._obj, "uniprot_offset")
            return list(filter(lambda x: x is not None, [ensembl_offset, refseq_offset, uniprot_offset]))
        else:
            return super().get(key, default)


class TargetGeneBase(BaseModel):
    """Base class for target gene view models."""

    name: str
    category: str
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetBase]

    class Config:
        getter_dict: ExternalIdentifiersGetter


class TargetGeneModify(TargetGeneBase):
    @validator("category")
    def validate_category(cls, v):
        target.validate_target_category(v)
        return v


class TargetGeneCreate(TargetGeneModify):
    """View model for creating a new target gene."""

    reference_maps: conlist(ReferenceMapCreate, min_items=1)
    wt_sequence: WildTypeSequenceCreate
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


class TargetGeneUpdate(TargetGeneModify):
    """View model for updating a target gene."""

    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


class SavedTargetGene(TargetGeneBase):
    """Base class for target gene view models representing saved records."""

    external_identifiers: list[external_gene_identifier_offset.SavedExternalGeneIdentifierOffset]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class TargetGene(SavedTargetGene):
    """Target gene view model containing a complete set of properties visible to non-admin users."""

    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter


class ShortTargetGene(SavedTargetGene):
    """Target gene view model containing a smaller set of properties to return in list contexts."""

    reference_maps: List[ReferenceMap]
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter


class AdminTargetGene(SavedTargetGene):
    """Target gene view model containing properties to return to admin clients."""

    creation_date: date
    modification_date: date
    reference_maps: List[ReferenceMap]
    wt_sequence: WildTypeSequence
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter
