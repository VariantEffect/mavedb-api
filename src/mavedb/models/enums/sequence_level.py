from enum import Enum


class SequenceLevel(str, Enum):
    """The molecular sequence level of a variant representation: genomic DNA, coding DNA, or protein.

    A single, duty-neutral closed set reused across several columns that each carry a different
    semantic meaning: the level a variant was *assayed* at (``assay_level``), the level dcd-mapping
    *aligned* it at (``alignment_level``), and the level of a stored allele (``level``).

    Values use full names so they round-trip readably through the database column; the dcd-mapping
    payload uses short single-character codes (``p`` / ``c`` / ``g``) which the worker translates via
    :func:`from_wire`.
    """

    protein = "protein"
    cdna = "cdna"
    genomic = "genomic"

    @classmethod
    def from_wire(cls, code: str) -> "SequenceLevel":
        """Translate a dcd-mapping single-character code into a ``SequenceLevel``."""
        try:
            return _WIRE_TO_LEVEL[code]
        except KeyError as exc:
            raise ValueError(f"Unknown sequence level wire code: {code!r}") from exc


# Module-level so it doesn't get mistaken for an enum member.
_WIRE_TO_LEVEL: dict[str, SequenceLevel] = {
    "p": SequenceLevel.protein,
    "c": SequenceLevel.cdna,
    "g": SequenceLevel.genomic,
}
