from enum import Enum

from pydantic import BaseModel

from .base.core import Method, iriReference, VariantPathogenicityProposition
from ..core.models import MappableConcept

class EvidenceOutcome(str, Enum):
    PS3 = "PS3"
    PS3_MODERATE = "PS3_moderate"
    PS3_SUPPORTING = "PS3_supporting"
    PS3_NOT_MET = "PS3_not_met"
    BS3 = "BS3"
    BS3_MODERATE = "BS3_moderate"
    BS3_SUPPORTING = "BS3_supporting"
    BS3_NOT_MET = "BS3_not_met"

class VariantPathogenicityFunctionalImpactEvidenceLine(BaseModel):
    targetProposition: VariantPathogenicityProposition | None
    strengthOfEvidenceProvided: MappableConcept | None
    specifiedBy: Method | iriReference | None
