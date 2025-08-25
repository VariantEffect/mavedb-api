from ga4gh.va_spec.base.core import (
    ExperimentalVariantFunctionalImpactStudyResult,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.dataset import score_set_to_data_set
from mavedb.lib.annotation.method import (
    publication_identifiers_to_method,
)
from mavedb.lib.annotation.document import variant_as_iri, mapped_variant_as_iri
from mavedb.lib.annotation.util import variation_from_mapped_variant


def mapped_variant_to_experimental_variant_impact_study_result(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactStudyResult:
    return ExperimentalVariantFunctionalImpactStudyResult(
        description=f"Variant effect study result for {mapped_variant.variant.urn}.",
        focusVariant=variation_from_mapped_variant(mapped_variant),
        functionalImpactScore=mapped_variant.variant.data["score_data"]["score"],  # type: ignore
        specifiedBy=publication_identifiers_to_method(
            mapped_variant.variant.score_set.publication_identifier_associations
        ),
        sourceDataSet=score_set_to_data_set(mapped_variant.variant.score_set),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
        ],
        reportedIn=filter(None, [variant_as_iri(mapped_variant.variant), mapped_variant_as_iri(mapped_variant)]),
    )
