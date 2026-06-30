import re
import sys
from typing import Optional

# Coordinate prefix of an HGVS variant description: a single type letter plus a dot
# (g. c. n. m. r. p. ...), capturing the prefix and the remaining description separately.
_HGVS_COORD_PREFIX = re.compile(r"^([a-z]\.)(.+)$")

_FIRST_INT = re.compile(r"\d+")


def _cis_phased_sort_key(description: str) -> tuple[int, str]:
    """Order key for a cis-phased component description by its first integer position.

    The first run of digits is the coordinate for both genomic (``123A>G``) and protein
    (``Arg123Gly``) forms. Descriptions with no digit sort last, and the raw string breaks ties so
    the order is total and stable.
    """
    match = _FIRST_INT.search(description)
    return (int(match.group()) if match else sys.maxsize, description)


def extract_accession(hgvs_string: str) -> str:
    """Extract the reference accession from an HGVS string, or return empty string if it cannot
    be parsed.

    This function makes no assumptions about the structure of the accession (e.g. whether it starts with "NM_"
    or "NC_") and is robust to extra whitespace. It simply returns the substring before the first colon, which
    is the standard HGVS separator between the accession and the variant description. Callers must validate
    the accession format separately if needed.
    """
    token = (hgvs_string or "").strip()
    if ":" not in token:
        return ""

    return token.split(":", 1)[0].strip()


_HGVS_P_PREDICTION = re.compile(r":p\.\((.+)\)\s*$")


def strip_protein_prediction_parens(hgvs_p: str) -> str:
    """Unwrap the prediction parentheses from a protein HGVS: ``p.(Ala222Val)`` -> ``p.Ala222Val``.

    Forward translation (``c_to_p``) emits a *predicted* consequence in parentheses. ga4gh
    translates either form fine, but the parens are noise we don't want in the stored HGVS --
    they denote inference, not a different variant -- so we normalize to the bare form for
    consistency. A string with no prediction parens is returned unchanged.
    """
    return _HGVS_P_PREDICTION.sub(r":p.\1", hgvs_p)


def split_cis_phased_hgvs(hgvs_string: str) -> list[str]:
    """Split a cis-phased multivariant HGVS expression into fully-qualified component strings.

    The reverse-translate-variants tool emits the non-adjacent component substitutions of a
    single codon change as one bracketed genomic expression (``NC_000001.11:g.[123A>G;125T>C]``)
    whenever they are too far apart on the genome to collapse into a single delins. ga4gh's
    AlleleTranslator only ever produces a single Allele, so each component must be translated
    independently and recombined into a VRS CisPhasedBlock.

    Each returned component carries the original accession and coordinate prefix
    (``NC_000001.11:g.123A>G``). A non-bracketed expression is returned unchanged as a
    single-element list, so callers can treat both cases uniformly.

    Unlike a bare mavehgvs split, the accession is preserved: the components feed straight
    into VRS translation, which requires a reference accession to resolve positions.
    """
    accession, separator, remainder = hgvs_string.partition(":")
    # Only an accession-qualified, bracketed expression is a cis-phased multivariant we split here;
    # anything else (bare, unbracketed, or accession-less) is returned unchanged so the caller can
    # treat both cases uniformly without a ValueError on the missing ":" / "[".
    if not separator or "[" not in remainder:
        return [hgvs_string]

    prefix = remainder[: remainder.index("[")]  # e.g. "g." / "c."
    inner = remainder[remainder.index("[") + 1 : remainder.rindex("]")]
    return [f"{accession}:{prefix}{component}" for component in inner.split(";") if component]


def join_cis_phased_hgvs(components: list[str]) -> Optional[str]:
    """Join cis-phased component HGVS strings into one bracketed expression.

    Inverse of :func:`split_cis_phased_hgvs`: ``["NC_…:g.123A>G", "NC_…:g.125T>C"]`` becomes
    ``"NC_…:g.[123A>G;125T>C]"``. A single component is returned unchanged.

    Returns ``None`` when the components do not share a single accession and coordinate prefix —
    they are then not expressible as one cis-phased block (e.g. members on different sequences).
    """
    if not components:
        return None
    if len(components) == 1:
        return components[0]

    accessions: set[str] = set()
    prefixes: set[str] = set()
    descriptions: list[str] = []
    for component in components:
        accession, separator, remainder = component.partition(":")
        match = _HGVS_COORD_PREFIX.match(remainder)
        if not separator or not match:
            return None

        accessions.add(accession)
        prefixes.add(match.group(1))
        descriptions.append(match.group(2))

    if len(accessions) != 1 or len(prefixes) != 1:
        return None

    # Emit components in coordinate order so the combined string is deterministic regardless of
    # member ordering. The VRS block digest is order-independent (so dedup is unaffected), but this
    # string is surfaced in CSV export, where a stable, spec-conventional ordering is useful.
    descriptions.sort(key=_cis_phased_sort_key)
    return f"{accessions.pop()}:{prefixes.pop()}[{';'.join(descriptions)}]"
