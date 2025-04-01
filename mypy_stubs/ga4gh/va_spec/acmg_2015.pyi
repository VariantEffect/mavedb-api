from mypy_stubs.ga4gh.va_spec.base.core import EvidenceLine, Method, iriReference, VariantPathogenicityProposition
from mypy_stubs.ga4gh.core.models import MappableConcept

class VariantPathogenicityFunctionalImpactEvidenceLine(EvidenceLine):
    targetProposition: VariantPathogenicityProposition | None
    strengthOfEvidenceProvided: MappableConcept | None
    specifiedBy: Method | iriReference | None
