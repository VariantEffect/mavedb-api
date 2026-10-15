"""Worker-side adapters for the variant_annotation translation ports.

construct_equivalent_variants depends on two protocols — CoordinateTranslator
and TranscriptSource. This module supplies the worker's CoordinateTranslator
(WorkerCoordinateTranslator) and a factory (uta_transcript_source) for the
TranscriptSource, which is variant_annotation's own UTA-backed UtaClient.

WorkerCoordinateTranslator defers AssemblyMapper initialization until the first
call — the hgvs library makes network calls on mapper construction that must not
fire in unit-test contexts where construct_equivalent_variants is mocked.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Generator
from typing import Any

from variant_annotation.lib.clients.uta import UtaClient

# The public UTA server refuses connections for minutes at a time; the client default (about 3 s in
# total) fails the job on a blip. Waits 2, 4, 8, 16 and 32 s before giving up.
UTA_MAX_ATTEMPTS = 6
UTA_BACKOFF_SECONDS = 2.0

_COMPLEMENT = str.maketrans("ACGTN", "TGCAN")


@contextlib.contextmanager
def uta_transcript_source() -> Generator[UtaClient]:
    """Yield a UTA-backed TranscriptSource over a connection scoped to the block.

    Backs both the NP_→NM_ association lookup and the WT-codon read used by
    WtCodonMode.ALL (TranscriptSource.codon_at). The client reconnects when UTA drops
    the connection and closes it on exit, so callers must use the client within the
    ``with`` block.
    """
    uta_db_url = (os.environ.get("UTA_DB_URL") or "").strip()
    if not uta_db_url:
        raise RuntimeError("UTA_DB_URL must be set to resolve transcript facts (NP_→NM_ associations and WT codons).")
    with UtaClient.from_url(uta_db_url, max_attempts=UTA_MAX_ATTEMPTS, backoff_seconds=UTA_BACKOFF_SECONDS) as client:
        yield client


def _reverse_complement(sequence: str) -> str:
    return sequence.translate(_COMPLEMENT)[::-1]


class WorkerCoordinateTranslator:
    """CoordinateTranslator backed by the worker's HGVS data provider."""

    def __init__(self, hdp: Any) -> None:
        self._hdp = hdp
        self._parser: Any = None
        self._mapper: Any = None
        self._literal_mapper: Any = None
        self._normalizer: Any = None

    def _ensure_initialized(self) -> None:
        if self._mapper is None:
            import hgvs.assemblymapper
            import hgvs.normalizer  # type: ignore[import-untyped]
            import hgvs.parser

            self._parser = hgvs.parser.Parser()
            self._normalizer = hgvs.normalizer.Normalizer(self._hdp)
            self._mapper = hgvs.assemblymapper.AssemblyMapper(
                self._hdp,
                assembly_name="GRCh38",
                alt_aln_method="splign",
            )
            # Unnormalized, so a delins stays a delins rather than becoming ``inv`` (see c_to_g_literal).
            self._literal_mapper = hgvs.assemblymapper.AssemblyMapper(
                self._hdp,
                assembly_name="GRCh38",
                alt_aln_method="splign",
                normalize=False,
            )

    def normalize(self, g_hgvs: str) -> str:
        """Trim and 3'-shift a genomic variant on its reference sequence, keeping it representable in VRS.

        A genomic delins that shares a base with its insertion, at an exon boundary, otherwise projects to
        an intron-spanning coding delins with no protein consequence instead of the coding deletion it is.
        hgvs rewrites a reverse-complement delins as ``inv``, which VRS cannot represent, so that is
        written back as the equivalent delins.
        """
        self._ensure_initialized()
        variant = self._normalizer.normalize(self._parser.parse(g_hgvs))
        if variant.posedit.edit.type == "inv":
            import hgvs.edit  # type: ignore[import-untyped]

            start, end = variant.posedit.pos.start.base, variant.posedit.pos.end.base
            ref = self._hdp.get_seq(variant.ac, start - 1, end).upper()
            variant.posedit.edit = hgvs.edit.NARefAlt(ref=ref, alt=_reverse_complement(ref))

        return str(variant)

    def c_to_p(self, c_hgvs: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.c_to_p(self._parser.parse(c_hgvs)))

    def g_to_c(self, g_hgvs: str, transcript: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.g_to_t(self._parser.parse(g_hgvs), transcript))

    def g_to_c_literal(self, g_hgvs: str, transcript: str) -> str:
        """Project to coding without normalization, so a delins stays a delins rather than becoming ``inv``."""
        self._ensure_initialized()
        return str(self._literal_mapper.g_to_t(self._parser.parse(g_hgvs), transcript))

    def c_to_g(self, c_hgvs: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.c_to_g(self._parser.parse(c_hgvs)))

    def mane_select_transcript(self, g_hgvs: str) -> str | None:
        """The MANE Select RefSeq transcript overlapping a genomic variant, or ``None`` if there is none.

        Reads cdot's per-build ``tag``, so it needs a cdot data provider. Where MANE Select transcripts of two
        genes overlap the variant, the lowest accession wins, so the choice is stable across runs. HGVS that
        does not parse has no locus and gives ``None``; data provider failures raise, so callers can tell an
        outage from a locus with no MANE transcript.
        """
        self._ensure_initialized()
        try:
            variant = self._parser.parse(g_hgvs)
        except Exception:
            return None

        candidates = sorted(
            tx["tx_ac"]
            for tx in self._hdp.get_tx_for_region(
                variant.ac, "splign", variant.posedit.pos.start.base - 1, variant.posedit.pos.end.base
            )
            if tx["tx_ac"].startswith("NM_")
        )
        for accession in candidates:
            build = self._hdp._get_transcript(accession).get("genome_builds", {}).get("GRCh38", {})
            if "MANE Select" in (build.get("tag") or ""):
                return accession

        return None

    def c_to_g_literal(self, c_hgvs: str) -> str:
        """Project to genomic without normalization; variant-annotation uses it to rewrite an ``inv`` as delins."""
        self._ensure_initialized()
        return str(self._literal_mapper.c_to_g(self._parser.parse(c_hgvs)))
