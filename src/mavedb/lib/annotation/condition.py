from ga4gh.core.models import Coding, iriReference as IRI, MappableConcept
from ga4gh.va_spec.base.domain_entities import Condition

from mavedb.lib.annotation.constants import GENERIC_DISEASE_MEDGEN_CODE, MEDGEN_SYSTEM


def generic_disease_condition_iri() -> IRI:
    return IRI(root=f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}")


def generic_disease_condition() -> Condition:
    return Condition(
        root=MappableConcept(
            conceptType="Disease",
            primaryCoding=Coding(
                code=GENERIC_DISEASE_MEDGEN_CODE,
                system=MEDGEN_SYSTEM,
                iris=[generic_disease_condition_iri()],
            ),
        )
    )
