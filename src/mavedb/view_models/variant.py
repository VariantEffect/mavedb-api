from datetime import date
from typing import Any, Optional

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.mapped_variant import SavedMappedVariant


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

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created. Only perform these
    # transformations if the relevant attributes are present on the input data (i.e., when creating from an ORM object).
    @model_validator(mode="before")
    def generate_associated_mapped_variant(cls, data: Any):
        if hasattr(data, "mapped_variants"):
            try:
                mapped_variant = next(
                    (mapped_variant for mapped_variant in data.mapped_variants if mapped_variant.current), None
                )
                data.__setattr__("mapped_variant", mapped_variant)
            except (AttributeError, KeyError) as exc:
                raise ValidationError(f"Unable to coerce mapped variant for {cls.__name__}: {exc}.")  # type: ignore
        return data


class VariantEffectMeasurement(SavedVariantEffectMeasurement):
    """Variant effect measurement view model returned to most clients"""

    pass
