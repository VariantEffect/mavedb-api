"""VRS allele identification helpers.

Centralizes the digest-correctness invariant for GA4GH VRS alleles: the
``ga4gh_identify`` Merkle tree caches sub-object digests on the object after
first identification, so any subsequent mutation (refgetAccession swap,
normalization, state coercion) leaves a stale id unless the cached digests
are cleared first. All allele identification in dcd_mapping must route
through :func:`identify_allele` so the digest is always recomputed from
current content.
"""

from typing import Any

from ga4gh.core import ga4gh_identify
from ga4gh.vrs.extras.translator import AlleleTranslator
from ga4gh.vrs.models import Allele, CisPhasedBlock, LiteralSequenceExpression, SequenceLocation
from ga4gh.vrs.normalize import normalize

from mavedb.lib.hgvs import split_cis_phased_hgvs


def translate_hgvs_to_vrs(hgvs: str, translator: AlleleTranslator) -> Allele:
    """Convert HGVS variation description to VRS object.

    The AlleleTranslator is supplied by the caller and reused across calls. ga4gh's
    AlleleTranslator opens a UTA connection lazily on first translate (via HgvsTools)
    and holds it for its lifetime, so constructing one per call opens — and leaks — a
    UTA connection per variant, exhausting the server's slot budget. Build one per
    job/worker and pass it in.

    :param hgvs: MAVE-HGVS variation string
    :param translator: caller-owned AlleleTranslator backed by a sequence/refget proxy
    :return: Corresponding VRS allele as a Pydantic class
    """
    # coerce tmp HGVS string into formally correct term
    if hgvs.startswith("NC_") and ":c." in hgvs:
        hgvs = hgvs.replace(":c.", ":g.")

    allele: Allele = translator.translate_from(hgvs, "hgvs", do_normalize=False)

    if (
        not isinstance(allele.location, SequenceLocation)
        or not isinstance(allele.location.start, int)
        or not isinstance(allele.location.end, int)
        or not isinstance(allele.state, LiteralSequenceExpression)
    ):
        raise ValueError

    return allele


def translate_hgvs_to_variation(hgvs: str, translator: AlleleTranslator) -> Allele | CisPhasedBlock:
    """Translate an HGVS expression — possibly a cis-phased multivariant — into a VRS object.

    Mirrors dcd_mapping's ``vrs_map._construct_vrs_allele``: each component HGVS is translated
    to an Allele independently; a single component returns a bare Allele, while two or more are
    wrapped in a CisPhasedBlock. The reverse-translation job emits bracketed genomic forms
    (``g.[a;b]``) for non-adjacent codon components that ga4gh's AlleleTranslator cannot
    translate directly, so splitting and recombining is the only way to represent them.

    The block's GA4GH digest is order-independent, so the same biological cis-phased set always
    identifies to one ``ga4gh:CPB.`` digest and dedups to a single row regardless of component
    ordering.

    :param hgvs: a single- or cis-phased-multivariant HGVS string
    :param translator: caller-owned AlleleTranslator reused across calls
    :return: an Allele for a single variant, or a CisPhasedBlock for a cis-phased set
    """
    members = [translate_hgvs_to_vrs(component, translator) for component in split_cis_phased_hgvs(hgvs)]
    if len(members) == 1:
        return members[0]

    block = CisPhasedBlock(members=members)  # type: ignore[call-arg]
    block.id = identify_variation(block)
    return block


def identify_allele(allele: Allele) -> str:
    """Clear cached digests and return a fresh GA4GH identifier for *allele*.

    ``ga4gh_identify`` is a Merkle-tree: it calls ``get_or_create_digest`` on
    sub-objects, returning any cached value without recomputing. Clearing both
    the location digest and the allele digest first ensures the id is always
    derived from the current object content — not from a value set before a
    refgetAccession mutation or normalization.
    """
    if isinstance(allele.location, SequenceLocation):
        allele.location.digest = None

    allele.digest = None
    digest = ga4gh_identify(allele)
    if digest is None:
        raise ValueError("Failed to compute GA4GH identifier for allele")  # noqa: EM101

    return digest


def identify_variation(variation: Allele | CisPhasedBlock) -> str:
    """Clear cached digests and return a fresh GA4GH id for an Allele or CisPhasedBlock.

    Generalizes :func:`identify_allele` to cis-phased blocks. A block's Merkle digest is
    derived from its members' digests, so a stale member digest would silently propagate into
    the block id. Clear every member (and its location) plus the block itself before
    identifying so the id always reflects current content.
    """
    if isinstance(variation, Allele):
        return identify_allele(variation)

    for member in variation.members:
        if isinstance(member, Allele):
            if isinstance(member.location, SequenceLocation):
                member.location.digest = None
            member.digest = None

    variation.digest = None
    digest = ga4gh_identify(variation)
    if digest is None:
        raise ValueError("Failed to compute GA4GH identifier for variation")  # noqa: EM101

    return digest


def normalize_and_identify(allele: Allele, data_proxy: Any) -> Allele:
    """Normalize *allele* and stamp it with a freshly computed GA4GH digest.

    Pairs the two finalize steps every VRS allele construction path needs.
    Routing identification through :func:`identify_allele` (rather than
    ``ga4gh_identify`` directly) is the invariant that protects against the
    Merkle-tree's stale-digest behavior after mutation -- so any allele
    construction site that bypasses this helper risks reintroducing the
    stale-digest bug.
    """
    allele = normalize(allele, data_proxy=data_proxy)
    allele.id = identify_allele(allele)
    return allele
