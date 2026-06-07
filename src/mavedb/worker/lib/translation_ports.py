"""Worker-side adapters for the variant_annotation translation ports.

construct_equivalent_variants depends on two protocols — CoordinateTranslator
and TranscriptSource. These are the worker's concrete implementations, so jobs
can pass them in without importing the port definitions directly.

WorkerCoordinateTranslator defers AssemblyMapper initialization until the first
call — the hgvs library makes network calls on mapper construction that must not
fire in unit-test contexts where construct_equivalent_variants is mocked.
"""

from __future__ import annotations

from typing import Any


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


class NullTranscriptSource:
    """Null TranscriptSource — the reverse-translation job resolves every coding transcript
    itself (including the UTA NP_→NM_ lookup) and always supplies it via VariantInput.transcript,
    so the library never needs to resolve one."""

    def transcript_for_protein(self, protein_accession: str) -> str | None:
        return None

    def codon_at(self, transcript: str, aa_position: int) -> str | None:
        return None
