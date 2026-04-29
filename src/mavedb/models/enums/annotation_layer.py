from enum import Enum


class AnnotationLayer(str, Enum):
    """Annotation layer for a variant mapping result.

    Mirrors the ``AnnotationLayer`` enum produced by the dcd-mapping QC API.
    Values use full names so they round-trip readably through the database
    column; the dcd-mapping payload uses short single-character codes
    (``p`` / ``c`` / ``g``) which the worker translates via :func:`from_wire`.
    """

    protein = "protein"
    cdna = "cdna"
    genomic = "genomic"

    @classmethod
    def from_wire(cls, code: str) -> "AnnotationLayer":
        """Translate a dcd-mapping single-character code into an ``AnnotationLayer``."""
        try:
            return _WIRE_TO_LAYER[code]
        except KeyError as exc:
            raise ValueError(f"Unknown annotation_level wire code: {code!r}") from exc


# Module-level so it doesn't get mistaken for an enum member.
_WIRE_TO_LAYER: dict[str, AnnotationLayer] = {
    "p": AnnotationLayer.protein,
    "c": AnnotationLayer.cdna,
    "g": AnnotationLayer.genomic,
}
