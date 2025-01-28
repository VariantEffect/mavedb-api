from datetime import date
from typing import Any

from mavedb.view_models.mapped_variant import MappedVariant
from pydantic.types import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class VariantBase(BaseModel):
    """Properties shared by most variant view models"""
    urn: Optional[str]
    data: Any
    score_set_id: int
    hgvs_nt: Optional[str]
    hgvs_pro: Optional[str]
    hgvs_splice: Optional[str]
    creation_date: date
    modification_date: date


class VariantCreate(VariantBase):
    """Input view model for creating variants"""
    pass


class VariantUpdate(VariantBase):
    """Input view model for updating variants"""
    pass


class SavedVariant(VariantBase):
    """Base class for variant view models handling saved variants"""
    id: int
    record_type: str = None  # type: ignore
    clingen_allele_id: Optional[str]

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


class Variant(SavedVariant):
    """Variant view model returned to most clients"""
    pass


class VariantWithShortScoreSet(SavedVariant):
    """Variant view model with mapped variants and a limited set of score set details"""
    score_set: "ShortScoreSet"
    mapped_variants: list[MappedVariant]


class ClingenAlleleIdVariantLookupsRequest(BaseModel):
    """A request to search for variants matching a list of ClinGen allele IDs"""
    clingen_allele_ids: list[str]


# ruff: noqa: E402
from mavedb.view_models.score_set import ShortScoreSet

VariantWithShortScoreSet.update_forward_refs()
