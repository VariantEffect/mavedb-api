from enum import Enum

from ..core.models import MappableConcept
from .base.core import EvidenceLine, Method, Statement, VariantPathogenicityProposition, iriReference

class VariantPathogenicityEvidenceLine(EvidenceLine):
    targetProposition: VariantPathogenicityProposition | None  # type: ignore
    strengthOfEvidenceProvided: MappableConcept | None
    specifiedBy: Method | iriReference | None

    class Criterion(str, Enum):
        """Define ACMG 2015 criterion values"""

        PVS1 = "PVS1"
        PS1 = "PS1"
        PS2 = "PS2"
        PS3 = "PS3"
        PS4 = "PS4"
        PM1 = "PM1"
        PM2 = "PM2"
        PM3 = "PM3"
        PM4 = "PM4"
        PM5 = "PM5"
        PM6 = "PM6"
        PP1 = "PP1"
        PP2 = "PP2"
        PP3 = "PP3"
        PP4 = "PP4"
        PP5 = "PP5"
        BA1 = "BA1"
        BS1 = "BS1"
        BS2 = "BS2"
        BS3 = "BS3"
        BS4 = "BS4"
        BP1 = "BP1"
        BP2 = "BP2"
        BP3 = "BP3"
        BP4 = "BP4"
        BP5 = "BP5"
        BP6 = "BP6"
        BP7 = "BP7"

class VariantPathogenicityStatement(Statement):
    proposition: VariantPathogenicityProposition

class AcmgClassification(str, Enum):
    PATHOGENIC = "pathogenic"
    BENIGN = "benign"
    LIKELY_PATHOGENIC = "likely pathogenic"
    LIKELY_BENIGN = "likely benign"
    UNCERTAIN_SIGNIFICANCE = "uncertain significance"
