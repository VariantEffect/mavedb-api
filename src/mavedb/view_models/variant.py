from datetime import date
from typing import TYPE_CHECKING, Any, Optional

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.mapped_variant import MappedVariant, SavedMappedVariant

if TYPE_CHECKING:
    from mavedb.view_models.score_set import ScoreSet, ShortScoreSet


class VariantEffectMeasurementBase(BaseModel):
    """Properties shared by most variant effect measurement view models"""

    urn: Optional[str] = None
    data: Any
    score_set_id: int
    hgvs_nt: Optional[str] = None
    hgvs_pro: Optional[str] = None
    hgvs_splice: Optional[str] = None
    creation_date: date
    modification_date: date


class VariantEffectMeasurementCreate(VariantEffectMeasurementBase):
    """Input view model for creating variant effect measurements"""

    pass


class VariantEffectMeasurementUpdate(VariantEffectMeasurementBase):
    """Input view model for updating variant effect measurements"""

    pass


class SavedVariantEffectMeasurement(VariantEffectMeasurementBase):
    """Base class for variant effect measurement view models handling saved variant effect measurements"""

    id: int
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


class SavedVariantEffectMeasurementWithMappedVariant(SavedVariantEffectMeasurement):
    """Class for saved variant effect measurement with any associated mapped variants"""

    mapped_variant: Optional[SavedMappedVariant] = None

    @model_validator(mode="before")
    def generate_score_set_urn_list(cls, data: Any):
        if not hasattr(data, "mapped_variant"):
            try:
                mapped_variant = None
                if data.mapped_variants:
                    mapped_variant = next(
                        mapped_variant for mapped_variant in data.mapped_variants if mapped_variant.current
                    )
                data.__setattr__("mapped_variant", mapped_variant)
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


class VariantEffectMeasurement(SavedVariantEffectMeasurement):
    """Variant effect measurement view model returned to most clients"""

    pass


class VariantEffectMeasurementWithScoreSet(SavedVariantEffectMeasurement):
    """Variant effect measurement view model with mapped variants and score set"""

    score_set: "ScoreSet"
    mapped_variants: list[MappedVariant]


class VariantEffectMeasurementWithShortScoreSet(SavedVariantEffectMeasurement):
    """Variant effect measurement view model with mapped variants and a limited set of score set details"""

    score_set: "ShortScoreSet"
    mapped_variants: list[MappedVariant]


class ClingenAlleleIdVariantLookupsRequest(BaseModel):
    """A request to search for variants matching a list of ClinGen allele IDs"""

    clingen_allele_ids: list[str]


class Variant(BaseModel):
    """View model for a variant, defined by its ClinGen allele id, with associated variant effect measurements"""

    clingen_allele_id: str
    variant_effect_measurements: list[VariantEffectMeasurementWithShortScoreSet]


class ClingenAlleleIdVariantLookupResponse(BaseModel):
    """Response model for a variant lookup by ClinGen allele ID"""

    clingen_allele_id: str
    exact_match: Optional[Variant] = None
    equivalent_nt: list[Variant] = []
    equivalent_aa: list[Variant] = []
