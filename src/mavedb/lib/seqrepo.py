"""
Utilities for working with SeqRepo.

See: https://github.com/biocommons/seqrepo-rest-service/blob/main/src/seqrepo_rest_service/utils.py
"""

import os
import re
from base64 import urlsafe_b64encode, urlsafe_b64decode
from binascii import unhexlify, hexlify

# TODO (https://github.com/VariantEffect/mavedb-api/issues/354). We need pydantic upgraded to use this package.
# from ga4gh.core.identifiers import is_ga4gh_identifier, CURIE_NAMESPACE as ga4gh_namespace
from typing import Union

from biocommons.seqrepo import SeqRepo, __version__ as seqrepo_dep_version
from bioutils.accessions import infer_namespaces


def base64url_to_hex(s: str) -> str:
    return hexlify(urlsafe_b64decode(s)).decode("ascii")


def hex_to_base64url(s: str) -> str:
    return urlsafe_b64encode(unhexlify(s)).decode("ascii")


def get_sequence_ids(sr: SeqRepo, query: str) -> list[str]:
    """determine sequence_ids after guessing form of query

    The query may be:
      * A fully-qualified sequence alias (e.g., VMC:0123 or refseq:NM_01234.5)
      * A digest or digest prefix from VMC, TRUNC512, or MD5
      * A sequence accession (without namespace)

    The first match will be returned.
    """

    nsa_options = _generate_nsa_options(query)
    for ns, a in nsa_options:
        aliases = list(sr.aliases.find_aliases(namespace=ns, alias=a))
        if aliases:
            break

    seq_ids = list(set(a["seq_id"] for a in aliases))
    return seq_ids


def _generate_nsa_options(query: str) -> Union[list[tuple[str, ...]], list[tuple[None, str]]]:
    """
    >>> _generate_nsa_options("NM_000551.3")
    [('refseq', 'NM_000551.3')]

    >>> _generate_nsa_options("ENST00000530893.6")
    [('ensembl', 'ENST00000530893.6')]

    >>> _generate_nsa_options("gi:123456789")
    [('gi', '123456789')]

    >> _generate_nsa_options("SQ.test")
    [('ga4gh', 'test')]

    >>> _generate_nsa_options("01234abcde")
    [('MD5', '01234abcde%'), ('VMC', 'GS_ASNKvN4=%')]

    """

    if ":" in query:
        # interpret as fully-qualified identifier
        nsa_options = [tuple(query.split(sep=":", maxsplit=1))]
        return nsa_options

    namespaces = infer_namespaces(query)
    if namespaces:
        nsa_options = [(ns, query) for ns in namespaces]
        return nsa_options

    # TODO (https://github.com/VariantEffect/mavedb-api/issues/354). We need pydantic upgraded to use this package.
    # if ga4gh, try ga4gh. GA4GH only accepts identifiers with a namespace prefix,
    # so we prepend the namespace when not present to test.
    # if is_ga4gh_identifier(f"{ga4gh_namespace}:{query}"):
    #     nsa_options = [("ga4gh", query)]
    #     return nsa_options

    # if hex, try MD5
    if re.match(r"^(?:[0-9A-Fa-f]{8,})$", query):
        nsa_options = [("MD5", query + "%")]
        # TRUNC512 isn't in seqrepo; synthesize equivalent VMC
        id_b64u = hex_to_base64url(query)
        nsa_options += [("VMC", "GS_" + id_b64u + "%")]
        return nsa_options

    return [(None, query)]


def seqrepo_versions() -> dict[str, str]:
    seqrepo_data_dir = os.getenv("HGVS_SEQREPO_DIR")
    if not seqrepo_data_dir:
        seqrepo_data_version = "unknown"
    else:
        seqrepo_data_version = seqrepo_data_dir.split(os.sep)[-1]  # last part of the path is the version

    return {
        "seqrepo_dependency_version": seqrepo_dep_version,
        "seqrepo_data_version": seqrepo_data_version,
    }
