from typing import Optional
from ga4gh.vrs.models import Allele, MolecularVariation
from ga4gh.va_spec.base import ExperimentalVariantFunctionalImpactProposition, VariantPathogenicityProposition
from ga4gh.va_spec.base.core import iriReference as IRI, MappableConcept
from ga4gh.core.models import Coding
from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.document import experiment_to_document
from mavedb.lib.annotation.study_result import (
    _variation_from_mapped_variant,
)


def mapped_variant_to_experimental_variant_clinical_impact_proposition(
    mapped_variant: MappedVariant,
) -> Optional[VariantPathogenicityProposition]:
    variation = _variation_from_mapped_variant(mapped_variant)

    return VariantPathogenicityProposition(
        description=f"Variant pathogenicity proposition for {mapped_variant.variant.urn}.",
        subjectVariant=MolecularVariation(Allele(**variation)),
        predicate="hasAssayVariantEffectFor",
        objectCondition=MappableConcept(
            conceptType="Absent",
            primaryCoding=Coding(
                code="Absent",
                system="Absent",
            ),
        ),
    )


def mapped_variant_to_experimental_variant_functional_impact_proposition(
    mapped_variant: MappedVariant,
) -> Optional[ExperimentalVariantFunctionalImpactProposition]:
    variation = _variation_from_mapped_variant(mapped_variant)

    return ExperimentalVariantFunctionalImpactProposition(
        description=f"Variant functional impact proposition for {mapped_variant.variant.urn}.",
        subjectVariant=MolecularVariation(Allele(**variation)),
        predicate="impactsFunctionOf",
        objectSequenceFeature=IRI(root="placeholder"),  # TODO: from post mapped target. This is dicey
        experimentalContextQualifier=experiment_to_document(mapped_variant.variant.score_set.experiment),
    )
