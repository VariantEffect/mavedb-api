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
from typing import Generator, Optional

from biocommons.seqrepo import SeqRepo, __version__ as seqrepo_dep_version
from bioutils.accessions import infer_namespaces


DEFAULT_CHUNK_SIZE = 8192


class SequenceNotFoundError(LookupError):
    """Raised when SeqRepo holds no sequence for an accession."""


class AmbiguousSequenceError(LookupError):
    """Raised when SeqRepo holds more than one sequence for an accession."""


def base64url_to_hex(s: str) -> str:
    return hexlify(urlsafe_b64decode(s)).decode("ascii")


def hex_to_base64url(s: str) -> str:
    return urlsafe_b64encode(unhexlify(s)).decode("ascii")


def get_sequence_ids(sr: SeqRepo, query: str) -> list[str]:
    """determine sequence_ids after guessing form of query

    The query may be:
      * A fully-qualified sequence alias (e.g., VMC:0123 or refseq:NM_01234.5)
      * A complete MD5 or TRUNC512 hex digest
      * A sequence accession (without namespace)

    The first match will be returned. Matching is exact: the shared SeqRepo holds the target sequences of
    unpublished score sets, so neither digest prefixes nor SQL wildcards may be used to enumerate it.
    """

    nsa_options = _generate_nsa_options(query)
    aliases: list[dict] = []
    for ns, a in nsa_options:
        aliases = list(sr.aliases.find_aliases(namespace=ns, alias=a))
        if aliases:
            break

    seq_ids = list(set(a["seq_id"] for a in aliases))
    return seq_ids


def resolve_refget(sr: SeqRepo, accession: str) -> str:
    """Return the single GA4GH refget (``SQ.…``) SeqRepo holds for an accession.

    SeqRepo's ``seq_id`` is the sequence's sha512t24u digest, so the refget is ``SQ.`` plus it. The
    shared SeqRepo is the only authority for what an accession's sequence is, and every writer of VRS
    alleles must agree with it for allele digests to deduplicate. More than one sequence for an accession
    is an error rather than a pick.

    The same contract is held by ``dcd_mapping.lookup.resolve_refget``; both are pinned to
    ``tests/fixtures/canonical_accession_sequences.json``.

    :raise SequenceNotFoundError: if SeqRepo has no sequence for the accession
    :raise AmbiguousSequenceError: if it has more than one
    """
    seq_ids = get_sequence_ids(sr, accession)
    if not seq_ids:
        raise SequenceNotFoundError(f"SeqRepo has no sequence for accession {accession}.")
    if len(seq_ids) > 1:
        raise AmbiguousSequenceError(f"SeqRepo holds {len(seq_ids)} sequences for accession {accession}.")

    return f"SQ.{seq_ids[0]}"


def _generate_nsa_options(query: str) -> list[tuple[Optional[str], str]]:
    """
    >>> _generate_nsa_options("NM_000551.3")
    [('refseq', 'NM_000551.3')]

    >>> _generate_nsa_options("ENST00000530893.6")
    [('ensembl', 'ENST00000530893.6')]

    >>> _generate_nsa_options("gi:123456789")
    [('gi', '123456789')]

    >> _generate_nsa_options("SQ.test")
    [('ga4gh', 'test')]

    >>> _generate_nsa_options("0123456789abcdef0123456789abcdef")
    [('MD5', '0123456789abcdef0123456789abcdef')]

    >>> _generate_nsa_options("NM_%")
    []

    """
    # SeqRepo switches to a LIKE comparison whenever an argument contains "%".
    if "%" in query:
        return []

    if ":" in query:
        # interpret as fully-qualified identifier
        namespace, alias = query.split(sep=":", maxsplit=1)
        return [(namespace, alias)]

    namespaces = infer_namespaces(query)
    if namespaces:
        return [(ns, query) for ns in namespaces]

    # TODO (https://github.com/VariantEffect/mavedb-api/issues/354). We need pydantic upgraded to use this package.
    # if ga4gh, try ga4gh. GA4GH only accepts identifiers with a namespace prefix,
    # so we prepend the namespace when not present to test.
    # if is_ga4gh_identifier(f"{ga4gh_namespace}:{query}"):
    #     nsa_options = [("ga4gh", query)]
    #     return nsa_options

    # A complete MD5 (32 hex characters) or TRUNC512 (48) digest. TRUNC512 isn't in SeqRepo; synthesize the
    # equivalent VMC alias.
    if re.fullmatch(r"[0-9A-Fa-f]{32}", query):
        return [("MD5", query.lower())]
    if re.fullmatch(r"[0-9A-Fa-f]{48}", query):
        return [("VMC", "GS_" + hex_to_base64url(query))]

    return [(None, query)]


def sequence_generator(
    sr: SeqRepo, seq_id: str, start: Optional[int], end: Optional[int], chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Generator[str, None, None]:
    """
    Generates sequence chunks from a SeqRepo sequence.

    Args:
        sr (SeqRepo): The SeqRepo instance to fetch sequences from.
        seq_id (str): The identifier of the sequence to retrieve.
        start (Optional[int]): The starting position (0-based, inclusive) of the sequence to fetch. If None, starts from 0.
        end (Optional[int]): The ending position (0-based, exclusive) of the sequence to fetch. If None, goes to the end of the sequence.
        chunk_size (int, optional): The size of each chunk to yield. Defaults to DEFAULT_CHUNK_SIZE.

    Yields:
        str: A chunk of the sequence as a string.

    Raises:
        Any exceptions raised by SeqRepo when fetching sequence information or sequence data.

    Example:
        for chunk in sequence_generator(sr, "seq1", 0, 1000, 100):
            process(chunk)
    """
    seq_len = sr.sequences.fetch_seqinfo(seq_id)["len"]
    seq_start = start if start is not None else 0
    seq_end = end if end is not None else seq_len

    for pos in range(seq_start, seq_end, chunk_size):
        chunk = sr.sequences.fetch(seq_id, pos, min(pos + chunk_size, seq_end))
        if not chunk:
            break
        yield chunk


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
