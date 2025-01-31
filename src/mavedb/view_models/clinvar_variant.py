# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ClinvarVariantBase(BaseModel):
    allele_id: int
    gene_symbol: str
    clinical_significance: str
    clinical_review_status: str
    clinvar_db_version: str


class ClinvarVariantCreate(ClinvarVariantBase):
    mapped_variants: Optional[list[MappedVariantCreate]] = None


class ClinvarVariantUpdate(ClinvarVariantBase):
    pass


# Properties shared by models stored in DB
class SavedClinvarVariant(ClinvarVariantBase):
    id: int
    modification_date: date
    creation_date: date
    #mapped_variants: Sequence[SavedMappedVariant]

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ClinvarVariant(SavedClinvarVariant):
    pass
    #mapped_variants: Sequence[MappedVariant]


# ruff: noqa: E402
from mavedb.view_models.mapped_variant import MappedVariant, SavedMappedVariant, MappedVariantCreate

ClinvarVariantCreate.update_forward_refs()
SavedClinvarVariant.update_forward_refs()
ClinvarVariant.update_forward_refs()
