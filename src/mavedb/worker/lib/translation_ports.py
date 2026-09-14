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

from variant_annotation.lib.clients.uta import UtaClient, connect_uta


@contextlib.contextmanager
def uta_transcript_source() -> Generator[UtaClient]:
    """Yield a UTA-backed TranscriptSource over a connection scoped to the block.

    Backs both the NP_→NM_ association lookup and the WT-codon read used by
    WtCodonMode.ALL (TranscriptSource.codon_at). The connection is closed on exit,
    so callers must use the client within the ``with`` block.
    """
    uta_db_url = (os.environ.get("UTA_DB_URL") or "").strip()
    if not uta_db_url:
        raise RuntimeError("UTA_DB_URL must be set to resolve transcript facts (NP_→NM_ associations and WT codons).")
    with contextlib.closing(connect_uta(uta_db_url)) as conn:
        yield UtaClient(conn)


class WorkerCoordinateTranslator:
    """CoordinateTranslator backed by the worker's HGVS data provider."""

    def __init__(self, hdp: Any) -> None:
        self._hdp = hdp
        self._parser: Any = None
        self._mapper: Any = None

    def _ensure_initialized(self) -> None:
        if self._mapper is None:
            import hgvs.assemblymapper
            import hgvs.parser

            self._parser = hgvs.parser.Parser()
            self._mapper = hgvs.assemblymapper.AssemblyMapper(
                self._hdp,
                assembly_name="GRCh38",
                alt_aln_method="splign",
            )

    def c_to_p(self, c_hgvs: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.c_to_p(self._parser.parse(c_hgvs)))

    def g_to_c(self, g_hgvs: str, transcript: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.g_to_t(self._parser.parse(g_hgvs), transcript))

    def c_to_g(self, c_hgvs: str) -> str:
        self._ensure_initialized()
        return str(self._mapper.c_to_g(self._parser.parse(c_hgvs)))
