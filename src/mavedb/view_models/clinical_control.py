# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ClinicalControlBase(BaseModel):
    db_identifier: str
    gene_symbol: str
    clinical_significance: str
    clinical_review_status: str
    db_version: str
    db_name: str


class ClinicalControlUpdate(ClinicalControlBase):
    mapped_variants: Optional[list[MappedVariantCreate]] = None


class ClinicalControlCreate(ClinicalControlUpdate):
    pass


# Properties shared by models stored in DB
class SavedClinicalControl(ClinicalControlBase):
    id: int
    modification_date: date
    creation_date: date

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


class SavedClinicalControlWithMappedVariants(SavedClinicalControl):
    mapped_variants: Sequence[SavedMappedVariant]


# Properties to return to non-admin clients
class ClinicalControl(SavedClinicalControl):
    pass


class ClinicalControlWithMappedVariants(SavedClinicalControlWithMappedVariants):
    mapped_variants: Sequence[MappedVariant]


class ClinicalControlOptions(BaseModel):
    db_name: str
    available_versions: list[str]


# ruff: noqa: E402
from mavedb.view_models.mapped_variant import MappedVariant, SavedMappedVariant, MappedVariantCreate

ClinicalControlCreate.update_forward_refs()
SavedClinicalControlWithMappedVariants.update_forward_refs()
ClinicalControlWithMappedVariants.update_forward_refs()
