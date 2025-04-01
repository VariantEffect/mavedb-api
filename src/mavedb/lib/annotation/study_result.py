from ga4gh.vrs.models import MolecularVariation, Allele
from ga4gh.va_spec.base import (
    ExperimentalVariantFunctionalImpactStudyResult,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
)
from mavedb.lib.annotation.dataset import score_set_to_data_set
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.method import (
    publication_identifiers_to_method,
)
from mavedb.lib.annotation.document import variant_as_iri


# NOTE: Early VRS 1.3 objects may contain an extra nesting level, where Allele objects
# are contained in a `variation` property. Although it's unlikely variants of this form
# will ever be exported in this format, we handle the possibility.
def _variation_from_mapped_variant(mapped_variant: MappedVariant) -> dict:
    if mapped_variant.post_mapped is None:
        raise MappingDataDoesntExistException(
            f"Variant {mapped_variant.variant.urn} does not have a post mapped variant."
            " Unable to extract variation data."
        )

    try:
        variation = mapped_variant.post_mapped["variation"]
    except KeyError:
        variation = mapped_variant.post_mapped

    variation_id = variation["location"].pop("sequence_id", None)
    if variation_id:
        variation["location"]["sequenceReference"] = variation_id

    variation_interval = variation["location"].pop("interval", None)
    if variation_interval:
        variation["location"]["start"] = variation_interval["start"]["value"]
        variation["location"]["end"] = variation_interval["end"]["value"]

    return variation


def mapped_variant_to_experimental_variant_impact_study_result(
    mapped_variant: MappedVariant,
) -> ExperimentalVariantFunctionalImpactStudyResult:
    variation = _variation_from_mapped_variant(mapped_variant)

    return ExperimentalVariantFunctionalImpactStudyResult(
        description=f"Variant effect study result for {mapped_variant.variant.urn}.",
        focusVariant=MolecularVariation(Allele(**variation)),
        functionalImpactScore=mapped_variant.variant.data["score_data"]["score"],  # type: ignore
        specifiedBy=publication_identifiers_to_method(
            mapped_variant.variant.score_set.publication_identifier_associations
        ),
        sourceDataSet=score_set_to_data_set(mapped_variant.variant.score_set),
        contributions=[
            mavedb_api_contribution(),
            mavedb_vrs_contribution(mapped_variant),
        ],
        reportedIn=variant_as_iri(mapped_variant.variant),
    )
