"""The MaveDB molecular-identity view of an allele — shared by variant detail and allele detail.

Both serving views present a set of alleles keyed by VRS digest, each labelled by how it relates to
the allele the view is anchored on (its *focus*). The two views differ only in what the focus is:
``GET /variants/{urn}`` focuses the **measured** allele, ``GET /alleles/{digest}`` focuses the
**queried** allele. The identity shape is shared and its labels are always read *relative to the
focus*.

This module owns that shared shape (:class:`AlleleIdentity`) and its provenance vocabulary
(:class:`AlleleDerivation`). The Cat-VRS structural relation codes live in :mod:`lib.cat_vrs`
(``CatVrsRelation``); this module carries only the derivation (confidence/provenance) axis, which is
orthogonal.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AlleleDerivation(str, Enum):
    """How an allele's representation was arrived at, *relative to the focus allele*: its
    confidence/provenance axis. Orthogonal to the Cat-VRS structural ``relation`` axis: ``relation``
    says which level relates to which, ``derivation`` says how much to trust the representation.

    There is deliberately **no** ``authoritative`` value: the focus allele is marked by
    :attr:`AlleleIdentity.is_focus`, not by a derivation. That keeps the axis meaningful without a
    measurement — on the allele view there is no *measured* allele, only the queried focus.
    """

    # Deterministic and precise, given (assembly, transcript): the focus's coordinate partner
    # (nucleotide↔nucleotide) and its protein consequence (nucleotide→protein).
    PROJECTION = "projection"
    # Reverse-translation of a *protein* focus: genuinely ambiguous. One member of the fanned-out
    # equivalence class, not a precise and deterministic pair.
    CANDIDATE = "candidate"
    # A distinct, precisely-known nucleotide change that *converges* on the same protein consequence
    # a different codon produces. Not ambiguous like a candidate nor a projection *of* the focus. A
    # separate, unmeasured variant that simply shares the consequence.
    CONVERGENT = "convergent"


@dataclass(frozen=True)
class AlleleIdentity:
    """The MaveDB molecular-identity facts for one allele in a view's ``alleles`` map, keyed by VRS
    digest. ``level`` + reference-frame ``hgvs`` (exactly one of the allele's genomic/coding/protein
    columns) are what the UI labels by — never the digest.

    Four axes, all read *relative to the view's focus allele*:

    - ``is_focus``: this allele is the one the view is anchored on (the measured allele on variant
      detail; the queried allele/CAID on allele detail). Its ``relation``/``derivation`` are ``None`` —
      it is the reference point the others are described against.
    - ``relation`` (Cat-VRS, structural): member→focus relation (``coordinate_representation_of`` /
      ``translation_of`` / ``encodes`` / ``co_encodes``). ``None`` for the focus itself.
    - ``derivation`` (provenance): :class:`AlleleDerivation` — projection / candidate / convergent.
      Orthogonal to ``relation``; never conflate them.
    - ``projection_of`` (provenance): the VRS digest of this allele's c↔g projection sibling, when
      known. ``None`` for the protein apex, pre-reverse-translation data, or where the pairing is not
      resolved in this view.
    """

    level: Optional[str]
    hgvs: Optional[str]
    clingen_allele_id: Optional[str]
    is_focus: bool
    relation: Optional[str]
    derivation: Optional[str]
    projection_of: Optional[str]
