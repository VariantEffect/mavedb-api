from ga4gh.core.models import Coding, MappableConcept
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactProposition, VariantPathogenicityProposition

from mavedb.lib.annotation.condition import generic_disease_condition
from mavedb.lib.annotation.document import experiment_to_document
from mavedb.lib.annotation.util import sequence_feature_for_mapped_variant, variation_from_mapped_variant
from mavedb.models.mapped_variant import MappedVariant


def mapped_variant_to_experimental_variant_clinical_impact_proposition(
    mapped_variant: MappedVariant,
) -> VariantPathogenicityProposition:
    coding, system = sequence_feature_for_mapped_variant(mapped_variant)
    sequence_feature = MappableConcept(
        primaryCoding=Coding(code=coding, system=system),
    )

    return VariantPathogenicityProposition(
        description=f"Variant pathogenicity proposition for {mapped_variant.variant.urn}.",
        subjectVariant=variation_from_mapped_variant(mapped_variant),
        predicate="isCausalFor",
        objectCondition=generic_disease_condition(),
        geneContextQualifier=sequence_feature
        if system == "https://www.genenames.org/"
        else None,  # only include gene context if we have a gene identifier
    )


def mapped_variant_to_experimental_variant_functional_impact_proposition(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactProposition:
    coding, system = sequence_feature_for_mapped_variant(mapped_variant)
    sequence_feature = MappableConcept(
        primaryCoding=Coding(code=coding, system=system),
    )

    return ExperimentalVariantFunctionalImpactProposition(
        description=f"Variant functional impact proposition for {mapped_variant.variant.urn}.",
        subjectVariant=variation_from_mapped_variant(mapped_variant),
        predicate="impactsFunctionOf",
        objectSequenceFeature=sequence_feature,
        experimentalContextQualifier=experiment_to_document(mapped_variant.variant.score_set.experiment),
    )
