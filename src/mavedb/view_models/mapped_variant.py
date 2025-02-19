# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Optional[Any]
    post_mapped: Optional[Any]
    variant_id: int
    vrs_version: Optional[str]
    error_message: Optional[str]
    modification_date: date
    mapped_date: date
    mapping_api_version: str
    current: bool


class MappedVariantCreate(MappedVariantBase):
    clinical_controles: Sequence[ClinicalControlBase]


class MappedVariantUpdate(MappedVariantBase):
    pass


# Properties shared by models stored in DB
class SavedMappedVariant(MappedVariantBase):
    id: int
    clinvar_variant: Sequence[SavedClinicalControl]
    clingen_allele_id: Optional[str]

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class MappedVariant(SavedMappedVariant):
    clinvar_variant: Sequence[ClinicalControl]


# ruff: noqa: E402
from mavedb.view_models.clinical_control import ClinicalControlBase, SavedClinicalControl, ClinicalControl

MappedVariantCreate.update_forward_refs()
SavedMappedVariant.update_forward_refs()
MappedVariant.update_forward_refs()
