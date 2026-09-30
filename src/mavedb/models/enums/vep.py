from enum import Enum


class VepConsequenceSource(str, Enum):
    """Which resolution path produced a stored VEP consequence — provenance, not severity.

    Persisted on every :class:`~mavedb.models.vep_allele_consequence.VepAlleleConsequence` so a stored
    consequence can be audited without re-querying Ensembl, and so a consumer can tell a
    transcript-specific call from a cross-transcript one, see #772.

    - ``transcript`` — read from the ``transcript_consequences`` entry matching the allele's own
      transcript. The trustworthy case: the consequence applies to the transcript the allele lives on.
    - ``most_severe`` — VEP's top-level ``most_severe_consequence``, the worst call across *every*
      transcript overlapping the position. Used only when no transcript could be matched (a genomic
      allele carrying no transcript, or a transcript absent from VEP's set). Lower confidence: it
      regularly reports a consequence that might not apply to the transcript of interest.
    - ``reference_identical`` — the allele describes no sequence change (a wild-type control); no VEP
      call was made. Recorded distinctly so it is never mistaken for ``synonymous_variant`` or a null.

    Values mirror ``variant_annotation.lib.vep.ConsequenceSource``: the library computes the source, the
    api owns this closed set. The two are kept in sync by
    :func:`mavedb.lib.vep.link_vep_consequences_to_alleles`, which maps one to the other by value at the
    write boundary.
    """

    transcript = "transcript"
    most_severe = "most_severe"
    reference_identical = "reference_identical"
