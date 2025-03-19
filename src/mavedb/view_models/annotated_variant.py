from typing import Optional
from ga4gh.va_spec.profiles import (
    AssayVariantEffectMeasurementStudyResult,
    AssayVariantEffectFunctionalClassificationStatement,
    AssayVariantEffectClinicalClassificationStatement,
)

from mavedb.view_models.base.base import BaseModel


class AnnotatedVariant(BaseModel):
    AssayVariantEffectMeasurementStudyResult: AssayVariantEffectMeasurementStudyResult
    AssayVariantEffectFunctionalClassificationStatement: Optional[AssayVariantEffectFunctionalClassificationStatement]
    AssayVariantEffectClinicalClassificationStatement: Optional[AssayVariantEffectClinicalClassificationStatement]
