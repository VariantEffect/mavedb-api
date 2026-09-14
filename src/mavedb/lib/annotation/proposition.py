from ga4gh.core.models import Coding, MappableConcept
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactProposition, VariantPathogenicityProposition

from mavedb.lib.annotation.condition import generic_disease_condition
from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.annotation.document import experiment_to_document
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata
from mavedb.lib.types.annotation import SequenceFeature
from mavedb.lib.variants import target_for_variant
from mavedb.models.variant import Variant


def sequence_feature_for_variant(variant: Variant) -> SequenceFeature:
    """
    Extract the sequence feature (e.g., gene or transcript) associated with a variant.

    This function retrieves the sequence feature from the variant's target data, which is
    necessary for generating annotations that reference specific genomic features. Co-located with the
    propositions that consume it — its only caller.

    Args:
        variant (Variant): The variant whose target gene supplies the sequence feature.

    Returns:
        SequenceFeature: Named tuple with:
            - `identifier`: sequence feature identifier (gene/transcript ID or name)
            - `system`: source/system URL for the identifier

    """
    target = target_for_variant(variant)
    if target is None:
        raise MappingDataDoesntExistException(
            f"Variant {variant.urn} does not have an identifiable target gene."
            " Unable to extract sequence feature for annotation."
        )

    # Prefer the mapped HGNC name if it's available, as this is more likely to be stable and recognizable than accessions or other identifiers.
    # If the mapped HGNC name is not available, fall back to extracting an identifier from the post-mapped metadata, which may be a gene or
    # transcript identifier of varying formats. If neither of those options are available, fall back to the target gene's name as listed in MaveDB.
    if target.mapped_hgnc_name:
        return SequenceFeature(target.mapped_hgnc_name, "https://www.genenames.org/")

    post_mapped_ids = extract_ids_from_post_mapped_metadata(
        target.post_mapped_metadata if target.post_mapped_metadata else {}  # type: ignore
    )
    if post_mapped_ids:
        post_mapped_id = post_mapped_ids[0]
        if post_mapped_id.startswith("ENSG") or post_mapped_id.startswith("ENST") or post_mapped_id.startswith("ENSP"):
            return SequenceFeature(post_mapped_id, "https://www.ensembl.org/index.html")
        elif post_mapped_id.startswith("NM_") or post_mapped_id.startswith("NR_") or post_mapped_id.startswith("NP_"):
            return SequenceFeature(post_mapped_id, "https://www.ncbi.nlm.nih.gov/refseq/")

        return SequenceFeature(post_mapped_id, "transcript or gene identifier of unknown source")

    if target.name:
        return SequenceFeature(target.name, "https://www.mavedb.org/")

    raise MappingDataDoesntExistException(
        f"Variant {variant.urn} does not have an identifiable sequence feature in its target gene data."
        " Unable to extract sequence feature for annotation."
    )


def variant_pathogenicity_proposition(
    context: VariantAnnotationContext,
) -> VariantPathogenicityProposition:
    coding, system = sequence_feature_for_variant(context.variant)
    sequence_feature = MappableConcept(
        primaryCoding=Coding(code=coding, system=system),
    )

    return VariantPathogenicityProposition(
        description=f"Variant pathogenicity proposition for {context.variant.urn}.",
        subjectVariant=context.subject_variant,
        predicate="isCausalFor",
        objectCondition=generic_disease_condition(),
        geneContextQualifier=sequence_feature
        if system == "https://www.genenames.org/"
        else None,  # only include gene context if we have a gene identifier
    )


def variant_functional_impact_proposition(
    context: VariantAnnotationContext,
) -> ExperimentalVariantFunctionalImpactProposition:
    coding, system = sequence_feature_for_variant(context.variant)
    sequence_feature = MappableConcept(
        primaryCoding=Coding(code=coding, system=system),
    )

    return ExperimentalVariantFunctionalImpactProposition(
        description=f"Variant functional impact proposition for {context.variant.urn}.",
        subjectVariant=context.subject_variant,
        predicate="impactsFunctionOf",
        objectSequenceFeature=sequence_feature,
        experimentalContextQualifier=experiment_to_document(context.variant.score_set.experiment),
    )
