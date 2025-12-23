# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Optional, Sequence

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class MappedVariantBase(BaseModel):
    pre_mapped: Optional[Any] = None
    post_mapped: Optional[Any] = None
    vrs_version: Optional[str] = None
    error_message: Optional[str] = None
    modification_date: date
    mapped_date: date
    mapping_api_version: str
    current: bool

    # Generated via model validators. On update/create classes, the input should be
    # a dict. On saved classes, the input should be a model instance.
    variant_urn: str


class MappedVariantUpdate(MappedVariantBase):
    clinical_controls: Sequence["ClinicalControlBase"]
    gnomad_variants: Sequence["GnomADVariantBase"]

    @model_validator(mode="before")
    def generate_score_set_urn_list(cls, data: Any):
        if "variant_urn" not in data and "variant" in data:
            try:
                data["variant_urn"] = None if not data["variant"] else data["variant"]["urn"]
            except KeyError as exc:
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

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created. Only perform these
    # transformations if the relevant attributes are present on the input data (i.e., when creating from an ORM object).
    @model_validator(mode="before")
    def generate_score_set_urn_list(cls, data: Any):
        if hasattr(data, "variant"):
            try:
                data.__setattr__("variant_urn", None if not data.variant else data.variant.urn)
            except (AttributeError, KeyError) as exc:
                raise ValidationError(f"Unable to coerce variant urn for {cls.__name__}: {exc}.")  # type: ignore
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


class MappedVariantForClinicalControl(BaseModel):
    variant_urn: str

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


# ruff: noqa: E402
from mavedb.view_models.clinical_control import ClinicalControl, ClinicalControlBase, SavedClinicalControl
from mavedb.view_models.gnomad_variant import GnomADVariant, GnomADVariantBase, SavedGnomADVariant

MappedVariantUpdate.model_rebuild()
SavedMappedVariantWithControls.model_rebuild()
MappedVariantWithControls.model_rebuild()
