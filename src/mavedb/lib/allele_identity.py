"""The MaveDB molecular-identity view of an allele — shared by variant detail and allele detail.

Both serving views present a set of alleles keyed by VRS digest, each labelled by how it relates to
the allele the view is anchored on (its *focus*). The two views differ only in what the focus is:
``GET /variants/{urn}`` focuses the **measured** allele, ``GET /alleles/{digest}`` focuses the
**queried** allele. The identity shape is shared and its labels are always read *relative to the
focus*.

This module owns that shared shape (:class:`AlleleIdentity`) and its provenance vocabulary
(:class:`AlleleDerivation`). The Cat-VRS structural relation codes live in :mod:`lib.cat_vrs`
(``CatVrsRelation``); this module carries the derivation (confidence/provenance) axis.

Why ``relation`` and ``derivation`` are separate axes
----------------------------------------------------
The two axes describe the *same* pair of alleles — member and focus — but ask questions from different
domains. ``relation`` is **biological**: how does this allele relate to the focus? ``derivation`` is
**provenance**: how did we arrive at this allele from the focus, and hence how much should it be
trusted? Neither answer determines the other, because a biological relationship does not say how it was
found and a route does not say what it found.

Half of that independence is already visible in the tree: ``projection`` covers *both* ``translation_of``
(nt→protein) and ``coordinate_representation_of`` (nt↔nt) — one route, two different biological
relationships. The other half is latent only because today exactly one pipeline path produces each
relationship. A ``co_encodes`` allele is biologically "a different codon encoding the same protein
change" however we reached it; reverse translation from the focus, a registry walk, and an independent
measurement in another score set are three different provenance claims about the same biology.

Two consequences that make the split load-bearing rather than tidy:

1. **Cat-VRS cannot express confidence, by design.** ``encodes`` is a claim about molecules: this
   nucleotide allele encodes that protein change. It is equally true of a codon we measured and one
   we guessed. A structural representation spec has no vocabulary for "we do not know which of these
   six codons was in the well".
2. **``candidate`` is a fact about our data, not about the molecule.** A protein-level assay reported
   an amino-acid change; which codon produced it is unknown *to MaveDB*, not unknown biologically.
   Attempting to encode that on a molecular-relationship axis would misstate what the molecule does.

Today's one-to-one mapping between the two vocabularies is **a fact about today's pipeline, not about
these axes. Never infer one from the other.** They are emitted together by
``variant_detail._derivation_for`` and ``allele_detail._member_label``, the single source of truth.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AlleleDerivation(str, Enum):
    """How an allele's representation was arrived at, *relative to the focus allele*: the
    confidence/provenance axis, and the one the UI badges on.

    There is deliberately **no** ``authoritative`` value: the focus allele is marked by
    :attr:`AlleleIdentity.is_focus`, not by a derivation. That keeps the axis meaningful even when a
    variant was not explicitly measured. See the module docstring for why this axis is separate from
    the Cat-VRS ``relation``, and why neither may be inferred from the other.
    """

    PROJECTION = "projection"
    """Deterministic and precise, given (assembly, transcript): the focus's projection
    (nucleotide↔nucleotide) and its protein consequence (nucleotide→protein)."""

    CANDIDATE = "candidate"
    """Reverse-translation of a *protein* focus: genuinely ambiguous. One member of the fanned-out
    equivalence class, not a precise and deterministic pair."""

    CONVERGENT = "convergent"
    """A distinct, precisely-known nucleotide change that *converges* on the same protein consequence
    a different codon produces. Not ambiguous like a candidate nor a projection *of* the focus. A
    separate, unmeasured variant that simply shares the consequence."""


@dataclass(frozen=True)
class AlleleIdentity:
    """The MaveDB molecular-identity facts for one allele in a view's ``alleles`` map, keyed by VRS
    digest.

    These four axes all read *relative to the view's focus allele*:

    - ``is_focus``: whether this allele is the one the view is anchored on. Its ``relation``/``derivation``
      are ``None``. This is the reference point the others are described against.
    - ``relation`` (Cat-VRS, biological): how this allele relates to the focus in a biological sense.
    - ``derivation`` (:class:`AlleleDerivation`, provenance): How we arrived at this allele from the focus,
      hence how much to trust it.
    - ``projection_of`` (provenance): the VRS digest of this allele's c↔g projection, when
      known. ``None`` for the protein apex, pre-reverse-translation data, or where the pairing is not
      resolved in this view.
    """

    is_focus: bool
    level: Optional[str]
    hgvs: Optional[str]
    clingen_allele_id: Optional[str]

    relation: Optional[str]
    derivation: Optional[AlleleDerivation]
    projection_of: Optional[str]
