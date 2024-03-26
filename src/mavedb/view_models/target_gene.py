from datetime import date
from typing import Any, Optional

from pydantic import root_validator
from pydantic.utils import GetterDict

from mavedb.view_models import external_gene_identifier_offset
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.target_sequence import TargetSequence, TargetSequenceCreate, SavedTargetSequence
from mavedb.view_models.target_accession import TargetAccession, TargetAccessionCreate, SavedTargetAccession
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

    target_sequence: Optional[TargetSequenceCreate]
    target_accession: Optional[TargetAccessionCreate]
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]

    @root_validator()
    def sequence_or_accession_required_and_mutually_exclusive(cls, values: dict[str, Any]) -> dict[str, Any]:
        target_seq, target_acc = values.get("target_sequence"), values.get("target_accession")
        if target_seq is not None and target_acc is not None:
            raise ValueError("Expected either a `target_sequence` or a `target_accession`, not both.")
        if target_seq is None and target_acc is None:
            raise ValueError("Expected either a `target_sequence` or a `target_accession`, not neither.")

        return values


class TargetGeneUpdate(TargetGeneModify):
    """View model for updating a target gene."""

    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


class SavedTargetGene(TargetGeneBase):
    """Base class for target gene view models representing saved records."""

    id: int
    target_sequence: Optional[SavedTargetSequence]
    target_accession: Optional[SavedTargetAccession]
    external_identifiers: list[external_gene_identifier_offset.SavedExternalGeneIdentifierOffset]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class TargetGene(SavedTargetGene):
    """Target gene view model containing a complete set of properties visible to non-admin users."""

    target_sequence: Optional[TargetSequence]
    target_accession: Optional[TargetAccession]
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter

    @validator("target_accession", always=True)
    def check_seq_or_accession(cls, target_accession, values):
        if "target_sequence" not in values and not target_accession:
            raise ValueError("either a `target_sequence` or `target_accession` is required")
        return target_accession


class ShortTargetGene(SavedTargetGene):
    """Target gene view model containing a smaller set of properties to return in list contexts."""

    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter


class AdminTargetGene(SavedTargetGene):
    """Target gene view model containing properties to return to admin clients."""

    creation_date: date
    modification_date: date
    target_sequence: Optional[list[TargetSequence]]
    target_accession: Optional[list[TargetAccession]]
    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]

    class Config:
        getter_dict = ExternalIdentifiersGetter

    @validator("target_accession", always=True)
    def check_seq_or_accession(cls, target_accession, values):
        if "target_sequence" not in values and not target_accession:
            raise ValueError("either a `target_sequence` or `target_accession` is required")
        return target_accession
