from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult

from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.dataset import score_set_to_data_set
from mavedb.lib.annotation.document import measured_allele_as_iri, variant_as_iri
from mavedb.lib.annotation.method import (
    publication_identifiers_to_method,
)
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from mavedb.lib.variants import variant_score


def variant_impact_study_result(
    context: VariantAnnotationContext,
) -> ExperimentalVariantFunctionalImpactStudyResult:
    # The study result's focus is the concrete measured allele — never a CategoricalVariant (VA-Spec
    # narrows ``focusVariant`` to MolecularVariation). The context guarantees a hydratable post_mapped.
    return ExperimentalVariantFunctionalImpactStudyResult(
        description=f"Variant effect study result for {context.variant.urn}.",
        # post_mapped is guaranteed non-null by variant_annotation_context (it returns None otherwise).
        focusVariant=vrs_object_from_mapped_variant(context.measured_allele.post_mapped),  # type: ignore[arg-type]
        functionalImpactScore=variant_score(context.variant),
        specifiedBy=publication_identifiers_to_method(context.variant.score_set.publication_identifier_associations),
        sourceDataSet=score_set_to_data_set(context.variant.score_set),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(context),
            mavedb_creator_contribution(context.variant, context.variant.score_set.created_by),
            mavedb_modifier_contribution(context.variant, context.variant.score_set.modified_by),
        ],
        reportedIn=filter(None, [variant_as_iri(context.variant), measured_allele_as_iri(context.measured_allele)]),
    )
