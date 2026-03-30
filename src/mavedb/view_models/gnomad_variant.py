# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel

if TYPE_CHECKING:
    from mavedb.view_models.mapped_variant import MappedVariant, MappedVariantCreate, SavedMappedVariant


class GnomADVariantBase(BaseModel):
    """Base class for GnomAD variant view models."""

    db_name: str
    db_identifier: str
    db_version: str

    allele_count: int
    allele_number: int
    allele_frequency: float

    faf95_max: Optional[float]
    faf95_max_ancestry: Optional[str]


class GnomADVariantUpdate(GnomADVariantBase):
    """View model for updating a GnomAD variant."""

    mapped_variants: Optional[list["MappedVariantCreate"]] = None


class GnomADVariantCreate(GnomADVariantBase):
    """View model for creating a new GnomAD variant."""

    pass


class SavedGnomADVariant(GnomADVariantBase):
    """Base class for GnomAD variant view models representing saved records."""

    record_type: str = None  # type: ignore

    id: int
    creation_date: date
    modification_date: date

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        arbitrary_types_allowed = True
        from_attributes = True


class SavedGnomADVariantWithMappedVariants(SavedGnomADVariant):
    """Saved GnomAD variant records with mapped variants."""

    mapped_variants: Sequence["SavedMappedVariant"]


class GnomADVariant(SavedGnomADVariant):
    """GnomAD variant view model for non-admin clients."""

    pass


class GnomADVariantWithMappedVariants(SavedGnomADVariantWithMappedVariants):
    """GnomAD variant view model with mapped variants for non-admin clients."""

    mapped_variants: Sequence["MappedVariant"]
