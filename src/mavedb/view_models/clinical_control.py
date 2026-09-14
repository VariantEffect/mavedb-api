# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Optional, Sequence

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel

if TYPE_CHECKING:
    from mavedb.view_models.mapped_variant import MappedVariantCreate


class ClinicalControlBase(BaseModel):
    db_identifier: str
    gene_symbol: str
    clinical_significance: str
    clinical_review_status: str
    db_version: str
    db_name: str


class ClinicalControlUpdate(ClinicalControlBase):
    mapped_variants: Optional[list["MappedVariantCreate"]] = None


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
        from_attributes = True


class ClinvarVariantLink(BaseModel):
    """One score-set variant a ClinVar control reaches, tagged with the annotated allele's digest."""

    variant_urn: str
    # VRS digest of the allele this control annotates. Many controls
    # may share the same variant_urn, but each will have a different allele_digest.
    allele_digest: Optional[str] = None


class SavedClinicalControlWithClinvarLinks(SavedClinicalControl):
    clinvar_links: Sequence[ClinvarVariantLink]


# Properties to return to non-admin clients
class ClinicalControl(SavedClinicalControl):
    pass


class ClinicalControlWithClinvarLinks(SavedClinicalControlWithClinvarLinks):
    pass


class ClinicalControlOptions(BaseModel):
    db_name: str
    available_versions: list[str]
