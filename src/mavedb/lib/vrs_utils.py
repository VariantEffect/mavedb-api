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
from ga4gh.vrs.models import Allele, LiteralSequenceExpression, SequenceLocation
from ga4gh.vrs.normalize import normalize


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
