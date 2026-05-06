import re
from typing import Any, Optional

from mavedb.lib.validation.constants.general import hgvs_columns
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant

HGVS_G_REGEX = re.compile(r"(^|:)g\.")
HGVS_P_REGEX = re.compile(r"(^|:)p\.")


def hgvs_from_vrs_allele(allele: dict) -> str:
    """
    Extract the HGVS notation from the VRS allele.
    """
    try:
        # VRS 2.X
        return allele["expressions"][0]["value"]
    except KeyError:
        if "variation" in allele:
            raise ValueError("VRS 1.X format not supported.")
            # VRS 1.X. We don't want to allow this.
            # return allele["variation"]["expressions"][0]["value"]
        else:
            raise KeyError("Invalid VRS allele structure. Expected 'expressions'.")


def get_hgvs_from_post_mapped(post_mapped_vrs: Optional[Any]) -> Optional[str]:
    if not post_mapped_vrs:
        return None

    if post_mapped_vrs["type"] == "Haplotype":  # type: ignore
        variations_hgvs = [hgvs_from_vrs_allele(allele) for allele in post_mapped_vrs["members"]]
    elif post_mapped_vrs["type"] == "CisPhasedBlock":  # type: ignore
        variations_hgvs = [hgvs_from_vrs_allele(allele) for allele in post_mapped_vrs["members"]]
    elif post_mapped_vrs["type"] == "Allele":  # type: ignore
        variations_hgvs = [hgvs_from_vrs_allele(post_mapped_vrs)]
    else:
        return None

    if len(variations_hgvs) == 0:
        return None
        # raise ValueError(f"No variations found in variant {variant_urn}.")

    # TODO (https://github.com/VariantEffect/mavedb-api/issues/468) In a future version, we will be able to generate
    # a combined HGVS string for haplotypes and cis phased blocks directly from mapper output.
    if len(variations_hgvs) > 1:
        return None
        # raise ValueError(f"Multiple variations found in variant {variant_urn}.")

    return variations_hgvs[0]


def get_digest_from_post_mapped(post_mapped_vrs: Optional[Any]) -> Optional[str]:
    """
    Extract the digest value from a post-mapped VRS object.

    Args:
        post_mapped_vrs: A post-mapped VRS (Variation Representation Specification) object
                        that may contain a digest field. Can be None.

    Returns:
        The digest string if present in the post_mapped_vrs object, otherwise None.
    """
    if not post_mapped_vrs:
        return None

    return post_mapped_vrs.get("digest")  # type: ignore


# TODO (https://github.com/VariantEffect/mavedb-api/issues/440) Temporarily, we are using these functions to distinguish
# genomic and protein HGVS strings produced by the mapper. Using hgvs.parser.Parser is too slow, and we won't need to do
# this once the mapper extracts separate g., c., and p. post-mapped HGVS strings.
def is_hgvs_g(hgvs: str) -> bool:
    """
    Check if the given HGVS string is a genomic HGVS (g.) string.
    """
    return bool(HGVS_G_REGEX.search(hgvs))


def is_hgvs_p(hgvs: str) -> bool:
    """
    Check if the given HGVS string is a protein HGVS (p.) string.
    """
    return bool(HGVS_P_REGEX.search(hgvs))


def target_for_variant(variant: Variant) -> Optional[TargetGene]:
    """
    Extract the appropriate target gene which the variant is reported against. In the case of single-target score sets, this
    is straightforwardly the target gene of the score set. In the case of multi-target score sets, we attempt to extract one of
    the post-mapped HGVS strings and use that to determine the appropriate target gene. If no post-mapped HGVS string is available, we return None.
    """
    score_set_targets = variant.score_set.target_genes
    if len(score_set_targets) == 1:
        return score_set_targets[0]

    # In multi-target score sets, hgvs strings are required to be fully qualified with respect to the target gene.
    # We can use this fact to determine the appropriate target gene for a variant by checking which target gene's
    # name or accession appears in the post-mapped HGVS string.
    hgvs_options = [getattr(variant, hgvs_attr) for hgvs_attr in hgvs_columns]
    for target in score_set_targets:
        qualifiers = []
        if getattr(target, "target_sequence", None) is not None and getattr(target.target_sequence, "label", None):
            qualifiers.append(target.target_sequence.label)
        if getattr(target, "target_accession", None) is not None and getattr(
            target.target_accession, "accession", None
        ):
            qualifiers.append(target.target_accession.accession)
        if any(
            hgvs_option and any(qualifier in hgvs_option for qualifier in qualifiers) for hgvs_option in hgvs_options
        ):
            return target

    return None
