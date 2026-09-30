"""Shared building block for GA4GH ``Coding`` objects drawn from a named term system.

A :data:`TermSystem` pairs a system URI/CURIE with the short prefix used to build a ``Coding.id`` for
codes drawn from it, kept together so a term's id and system can't drift apart the way two independent
lookup tables could.
"""

from ga4gh.core.models import Coding

TermSystem = tuple[str, str]


### MaveDB Term Systems

# MaveDB's own Cat-VRS relation codes (member -> defining), in cat_vrs.py's own framing rather than the
# spec's.
MAVEDB_CAT_VRS_RELATION: TermSystem = ("https://mavedb.org/cat-vrs/relations", "mavedb")


### External Term Systems

# Sequence Ontology. Used by several Cat-VRS Relation terms (translation_of, transcribed_to); see
# ga4gh/cat-vrs examples/json/proteinSequenceConsequence-ex2.json.
SEQUENCE_ONTOLOGY: TermSystem = ("http://www.sequenceontology.org", "so")

# Cat-VRS's own internally controlled vocabulary for the allele-relation terms that have no external
# ontology equivalent (currently just liftover_to). See ga4gh/cat-vrs recipes-source.yaml. Other
# ga4gh-gks-term categories (e.g. experimental-var-func-impact-classification) are separate term
# systems under the same vocabulary owner, not this one.
GKS_ALLELE_RELATION: TermSystem = ("ga4gh-gks-term:allele-relation", "ga4gh-gks-term")


def coding(term_system: TermSystem, code: str) -> Coding:
    """Build a ``Coding`` for `code`, drawn from `term_system`."""
    uri, prefix = term_system
    return Coding(id=f"{prefix}:{code}", code=code, system=uri)
