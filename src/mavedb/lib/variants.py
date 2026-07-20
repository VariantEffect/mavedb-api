import re
from typing import Any, Optional

from mavedb.lib.hgvs import join_cis_phased_hgvs
from mavedb.lib.mave.constants import REQUIRED_SCORE_COLUMN, VARIANT_SCORE_DATA
from mavedb.lib.validation.constants.general import hgvs_columns
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant


def score_from_variant_data(data: Optional[Any]) -> Optional[float]:
    """The canonical numeric score in a variant's ``data`` JSONB (``score_data.score``), or ``None``.

    The score column is required for every score set, but an individual variant may carry a null/NA
    score. Returns ``None`` when the score is absent, null, or not coercible to a float; a numeric
    string (``"1.5"``) coerces, but ``bool`` is rejected — a JSON ``true`` is not a score. Robust to
    malformed JSONB: a non-mapping ``data`` or ``score_data`` yields ``None`` rather than raising.
    Operates on the ``data`` mapping directly so it serves both ORM objects and bare ``Variant.data``
    column reads.
    """
    if not isinstance(data, dict):
        return None

    score_data = data.get(VARIANT_SCORE_DATA)
    if not isinstance(score_data, dict):
        return None

    value = score_data.get(REQUIRED_SCORE_COLUMN)
    if value is None or isinstance(value, bool):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def variant_score(variant: Variant) -> Optional[float]:
    """The canonical numeric score of a variant (see :func:`score_from_variant_data`)."""
    return score_from_variant_data(variant.data)


HGVS_G_REGEX = re.compile(r"(^|:)g\.")
HGVS_P_REGEX = re.compile(r"(^|:)p\.")


def hgvs_from_vrs_allele(allele: dict) -> Optional[str]:
    """
    Extract the HGVS notation from the VRS allele, or None if it carries no expression.
    """
    try:
        expressions = allele["expressions"]  # VRS 2.X
    except KeyError:
        if "variation" in allele:
            raise ValueError("VRS 1.X format not supported.")
            # VRS 1.X. We don't want to allow this.
        raise KeyError("Invalid VRS allele structure. Expected 'expressions'.")

    # A valid VRS allele may simply carry no HGVS expression (None or empty) — e.g. a member of a
    # cis-phased block. That is "no HGVS", not a crash.
    if not expressions:
        return None
    return expressions[0]["value"]


def get_hgvs_from_post_mapped(post_mapped_vrs: Optional[Any], *, combine_cis: bool = False) -> Optional[str]:
    """Extract a single HGVS string from a post-mapped VRS object.

    Multi-variant blocks (Haplotype/CisPhasedBlock) are cis-phased, so their members combine
    into one bracketed expression (``NC_…:g.[a;b]``) when ``combine_cis`` is set. It defaults
    off because some consumers cannot yet handle a bracketed expression — notably ClinGen
    submission, which has no single CAID for a multi-variant cis block (see
    https://github.com/VariantEffect/mavedb-api/issues/764).
    """
    if not post_mapped_vrs:
        return None

    if post_mapped_vrs["type"] in ("Haplotype", "CisPhasedBlock"):  # type: ignore
        members = post_mapped_vrs["members"]
    elif post_mapped_vrs["type"] in ("Allele", "VariationDescriptor"):  # type: ignore
        members = [post_mapped_vrs]
    else:
        return None

    member_hgvs = [hgvs_from_vrs_allele(allele) for allele in members]

    # No members, or a member carrying no HGVS expression — no single/combinable HGVS to return.
    if not member_hgvs or any(h is None for h in member_hgvs):
        return None

    hgvs_values: list[str] = [h for h in member_hgvs if h is not None]
    if len(hgvs_values) > 1:
        return join_cis_phased_hgvs(hgvs_values) if combine_cis else None

    return hgvs_values[0]


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
