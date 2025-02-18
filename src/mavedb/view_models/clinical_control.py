# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ClinicalControlBase(BaseModel):
    db_identifier: int
    gene_symbol: str
    clinical_significance: str
    clinical_review_status: str
    db_version: str
    db_name: str


class ClinicalControlCreate(ClinicalControlBase):
    mapped_variants: Optional[list[MappedVariantCreate]] = None


class ClinicalControlUpdate(ClinicalControlBase):
    pass


# Properties shared by models stored in DB
class SavedClinicalControl(ClinicalControlBase):
    id: int
    modification_date: date
    creation_date: date
    mapped_variants: Sequence[SavedMappedVariant]

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ClinicalControl(SavedClinicalControl):
    mapped_variants: Sequence[MappedVariant]


# ruff: noqa: E402
from mavedb.view_models.mapped_variant import MappedVariant, SavedMappedVariant, MappedVariantCreate

ClinicalControlCreate.update_forward_refs()
SavedClinicalControl.update_forward_refs()
ClinicalControl.update_forward_refs()
