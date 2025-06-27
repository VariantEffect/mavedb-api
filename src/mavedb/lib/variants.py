import re
from typing import Any, Optional


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
