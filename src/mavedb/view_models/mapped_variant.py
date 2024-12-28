# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Optional, Sequence

from pydantic import model_validator

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Optional[Any] = None
    post_mapped: Optional[Any] = None
    variant_urn: str
    vrs_version: Optional[str] = None
    error_message: Optional[str] = None
    modification_date: date
    mapped_date: date
    mapping_api_version: str
    current: bool


class MappedVariantUpdate(MappedVariantBase):
    clinical_controls: Sequence["ClinicalControlBase"]
    gnomad_variants: Sequence["GnomADVariantBase"]

    @model_validator(mode="before")
    def generate_score_set_urn_list(cls, data: Any):
        if not "variant_urn" in data and "variant" in data:
            try:
                data.__setattr__("variant_urn", None if not data["variant"] else data["variant"]["urn"])
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


class MappedVariantCreate(MappedVariantUpdate):
    pass


# Properties shared by models stored in DB
class SavedMappedVariant(MappedVariantBase):
    id: int
    clingen_allele_id: Optional[str] = None

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True

    @model_validator(mode="before")
    def generate_score_set_urn_list(cls, data: Any):
        if not hasattr(data, "variant_urn") and hasattr(data, "variant"):
            try:
                data.__setattr__("variant_urn", None if not data.variant else data.variant.urn)
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


class SavedMappedVariantWithControls(SavedMappedVariant):
    clinical_controls: Sequence["SavedClinicalControl"]
    gnomad_variants: Sequence["SavedGnomADVariant"]


# Properties to return to non-admin clients
class MappedVariant(SavedMappedVariant):
    pass


class MappedVariantWithControls(SavedMappedVariantWithControls):
    clinical_controls: Sequence["ClinicalControl"]
    gnomad_variants: Sequence["GnomADVariant"]


# ruff: noqa: E402
from mavedb.view_models.clinical_control import ClinicalControlBase, ClinicalControl, SavedClinicalControl
from mavedb.view_models.gnomad_variant import GnomADVariantBase, GnomADVariant, SavedGnomADVariant

MappedVariantUpdate.model_rebuild()
SavedMappedVariantWithControls.model_rebuild()
MappedVariantWithControls.model_rebuild()
