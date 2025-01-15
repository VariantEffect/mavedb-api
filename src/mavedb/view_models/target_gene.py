from datetime import date
from typing import Any, Optional, Sequence, Literal
from typing_extensions import TypedDict, Self

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import transform_external_identifier_offsets_to_list
from mavedb.models.enums.target_category import TargetCategory
from mavedb.view_models import external_gene_identifier_offset, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.target_accession import SavedTargetAccession, TargetAccession, TargetAccessionCreate
from mavedb.view_models.target_sequence import (
    AdminTargetSequence,
    SavedTargetSequence,
    TargetSequence,
    TargetSequenceCreate,
)


class ReferenceSequence(TypedDict):
    sequence_type: str
    sequence_id: str


class PreMappedReferenceSequence(ReferenceSequence):
    pass


class PostMappedReferenceSequence(ReferenceSequence):
    sequence_accessions: list[str]


class TargetGeneBase(BaseModel):
    """Base class for target gene view models."""

    name: str
    category: TargetCategory
    external_identifiers: Sequence[external_gene_identifier_offset.ExternalGeneIdentifierOffsetBase]


class TargetGeneModify(TargetGeneBase):
    pass


class TargetGeneCreate(TargetGeneModify):
    """View model for creating a new target gene."""

    target_sequence: Optional[TargetSequenceCreate] = None
    target_accession: Optional[TargetAccessionCreate] = None
    external_identifiers: Sequence[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]

    @model_validator(mode="after")
    def sequence_or_accession_required_and_mutually_exclusive(self) -> Self:
        if self.target_sequence is not None and self.target_accession is not None:
            raise ValueError("Expected either a `target_sequence` or a `target_accession`, not both.")
        if self.target_sequence is None and self.target_accession is None:
            raise ValueError("Expected either a `target_sequence` or a `target_accession`, not neither.")
        return self


class TargetGeneUpdate(TargetGeneModify):
    """View model for updating a target gene."""

    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffsetCreate]


class SavedTargetGene(TargetGeneBase):
    """Base class for target gene view models representing saved records."""

    id: int
    record_type: str = None  # type: ignore
    target_sequence: Optional[SavedTargetSequence] = None
    target_accession: Optional[SavedTargetAccession] = None
    external_identifiers: Sequence[external_gene_identifier_offset.SavedExternalGeneIdentifierOffset]
    pre_mapped_metadata: Optional[dict[Literal["protein", "genomic"], PreMappedReferenceSequence]] = None
    post_mapped_metadata: Optional[dict[Literal["protein", "genomic"], PostMappedReferenceSequence]] = None
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created.
    @model_validator(mode="before")
    def generate_external_identifiers_list(cls, data: Any):
        try:
            data.__setattr__("external_identifiers", transform_external_identifier_offsets_to_list(data))
        except AttributeError as exc:
            raise ValidationError(
                f"Unable to create {cls.__name__} without attribute: {exc}."  # type: ignore
            )
        return data

    @model_validator(mode="after")
    def check_seq_or_accession(self) -> Self:
        if self.target_sequence is None and self.target_accession is None:
            raise ValidationError("Either a `target_sequence` or `target_accession` is required.")
        return self


class TargetGene(SavedTargetGene):
    """Target gene view model containing a complete set of properties visible to non-admin users."""

    target_sequence: Optional[TargetSequence] = None
    target_accession: Optional[TargetAccession] = None
    external_identifiers: Sequence[external_gene_identifier_offset.ExternalGeneIdentifierOffset]


class ShortTargetGene(TargetGene):
    """Target gene view model containing a smaller set of properties to return in list contexts."""

    external_identifiers: list[external_gene_identifier_offset.ExternalGeneIdentifierOffset]


class AdminTargetGene(SavedTargetGene):
    """Target gene view model containing properties to return to admin clients."""

    creation_date: date
    modification_date: date
    target_sequence: Optional[AdminTargetSequence] = None
    target_accession: Optional[TargetAccession] = None
    external_identifiers: Sequence[external_gene_identifier_offset.ExternalGeneIdentifierOffset]
