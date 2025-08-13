from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactProposition, VariantPathogenicityProposition
from ga4gh.core.models import iriReference as IRI

from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.condition import generic_disease_condition
from mavedb.lib.annotation.document import experiment_to_document
from mavedb.lib.annotation.util import variation_from_mapped_variant


def mapped_variant_to_experimental_variant_clinical_impact_proposition(
    mapped_variant: MappedVariant,
) -> VariantPathogenicityProposition:
    return VariantPathogenicityProposition(
        description=f"Variant pathogenicity proposition for {mapped_variant.variant.urn}.",
        subjectVariant=variation_from_mapped_variant(mapped_variant),
        predicate="isCausalFor",
        objectCondition=generic_disease_condition(),
    )


def mapped_variant_to_experimental_variant_functional_impact_proposition(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactProposition:
    return ExperimentalVariantFunctionalImpactProposition(
        description=f"Variant functional impact proposition for {mapped_variant.variant.urn}.",
        subjectVariant=variation_from_mapped_variant(mapped_variant),
        predicate="impactsFunctionOf",
        objectSequenceFeature=IRI(root="placeholder"),  # TODO: from post mapped target. This is dicey
        experimentalContextQualifier=experiment_to_document(mapped_variant.variant.score_set.experiment),
    )
