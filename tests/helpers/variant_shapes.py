"""The mapped-variant shapes every export surface has to survive.

Tests otherwise all run against one variant, so a defect that only appears for a particular stored
payload passes the whole suite. Both production failures this list exists to catch were of that kind: a
reference-identical variant whose VRS state is a ``ReferenceLengthExpression`` (84125081) and a null
baseline score stripped on the way out (5c155f4d).

Consumed by both the annotation surfaces (``tests/lib/annotation/test_conformance.py``) and the CSV
composer (``tests/lib/csv/test_columns.py``), which is why it lives here rather than in either package.

Adding a shape is one entry in ``VARIANT_SHAPES``, and it applies to every surface at once. Not every
shape bears on every surface: ``unmapped_hgvs_columns`` changes only CSV output, and the calibration
shapes change only annotation output. That is fine — a shape that is inert on one surface costs one
cheap assertion there and earns its place on the other.

Two things are deliberately not shapes here:

- **gnomAD records and ClinVar controls.** The CSV composer does read both, but as separate per-row
  arguments rather than as properties of a mapped variant, so varying them is an orthogonal axis. The
  annotation layer reads neither.
- **``score_data`` with no ``score`` key.** Score dataframes are rejected without a ``score`` column
  (``lib/validation/dataframe``), so a stored variant always has the key; it may be null, which is
  ``null_score``.

Expected to be short-lived. This builds on the non-DB mock factories in ``tests/helpers/mocks/factories.py``,
hand-rolling the override plumbing — ``kwargs`` forwarded to a factory, plus a ``mutate`` hook for the axes
a factory signature cannot reach. #782 is an open decision on how the suite should construct test objects at
all (``factory_boy`` versus explicit scenario builders). If ``factory_boy`` wins, ``VariantShape.kwargs``
becomes factory params or traits and ``mutate`` becomes a post-generation hook, and this module reduces
to the list itself. The parametrize-over-a-list structure is worth keeping either way; the plumbing here
is not.

Note that #782 is chiefly about *DB-backed* construction and explicitly keeps that layer distinct from
these mock factories, so adoption there does not automatically retire this — but the two should be
reconciled rather than left to drift.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from tests.helpers.constants import (
    TEST_VALID_POST_MAPPED_VRS_ALLELE,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_DIGEST_ONLY,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_GENOMIC,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_LENGTH_EXPRESSION,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_RLE,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS1_X,
    TEST_VALID_PRE_MAPPED_VRS_CIS_PHASED_BLOCK,
)

# Every shape sets this unless it is the axis under test, so that a variant carrying no ClinGen id is a
# deliberate case rather than an accident of the factory default.
DEFAULT_CLINGEN_ALLELE_ID = "CA123456"


@dataclass(frozen=True)
class VariantShape:
    """One mapped-variant configuration, and how to build it from a mock factory."""

    name: str
    why: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    #: Applied after construction, for axes the factory signature does not reach.
    mutate: Optional[Callable[[Any], None]] = None

    def build(self, factory: Callable[..., Any]):
        """Build this shape with *factory*, one of the ``create_mock_mapped_variant*`` functions."""
        mapped_variant = factory(**{"clingen_allele_id": DEFAULT_CLINGEN_ALLELE_ID, **self.kwargs})
        if self.mutate is not None:
            self.mutate(mapped_variant)
        return mapped_variant


def _only_target_gene(mapped_variant):
    return mapped_variant.variant.score_set.target_genes[0]


def _drop_gene_symbol(mapped_variant) -> None:
    """Force the fallback from the mapped HGNC name down to the target's own name.

    ``post_mapped_metadata`` has to be set explicitly: the mock factory leaves it unset, and an unset
    attribute on a MagicMock is a truthy mock rather than the empty metadata a real target would have.
    """
    target = _only_target_gene(mapped_variant)
    target.mapped_hgnc_name = None
    target.post_mapped_metadata = {}


def _drop_baseline_score(mapped_variant) -> None:
    """A calibration with no baseline score.

    ``Extension.value`` is required, so an extension built around a null baseline score was stripped by
    ``model_dump(exclude_none=True)`` and the emitted object then refused to re-parse. That is the defect
    5c155f4d fixed, and this is the shape that reaches it.
    """
    for calibration in mapped_variant.variant.score_set.score_calibrations:
        calibration.baseline_score = None
        calibration.baseline_score_description = None


def _make_non_coding(mapped_variant) -> None:
    target = _only_target_gene(mapped_variant)
    target.category = "Regulatory"
    target.mapped_hgnc_name = None
    target.post_mapped_metadata = {"genomic": {"sequence_id": "ga4gh:SQ.test"}}


VARIANT_SHAPES: list[VariantShape] = [
    VariantShape(
        name="vrs2_allele",
        why="the current default: a VRS 2.x allele with a literal sequence state",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X},
    ),
    VariantShape(
        name="vrs1_allele",
        why="VRS 1.x nests the allele under a `variation` key",
        kwargs={
            "pre_mapped": TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS1_X,
            "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS1_X,
        },
    ),
    VariantShape(
        name="protein_allele",
        why="an hgvs.p expression carrying neither `type` nor `syntax_version`",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE},
    ),
    VariantShape(
        name="genomic_expression",
        why="an hgvs.g expression against a chromosome accession rather than a protein one",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_GENOMIC},
    ),
    VariantShape(
        name="reference_length_expression",
        why="reference-identical variants store an RLE state; 84125081 fixed a 500 on exactly this",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_RLE},
    ),
    VariantShape(
        name="length_expression",
        why="a LengthExpression state, the third of the three states util.py accepts",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_LENGTH_EXPRESSION},
    ),
    VariantShape(
        name="cis_phased_block",
        why="a haplotype resolves to a CisPhasedBlock rather than a bare Allele",
        kwargs={
            "pre_mapped": TEST_VALID_PRE_MAPPED_VRS_CIS_PHASED_BLOCK,
            "post_mapped": TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
        },
    ),
    VariantShape(
        name="digest_only_post_mapped",
        why="an allele with no expressions and no reference-sequence extension",
        kwargs={"post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_DIGEST_ONLY},
    ),
    VariantShape(
        name="null_score",
        why="an NA score, which `exclude_none=True` strips on the way out",
        kwargs={"score": None},
    ),
    VariantShape(
        name="absent_baseline_score",
        why="a calibration with no baseline score; 5c155f4d fixed output that no longer re-parsed",
        mutate=_drop_baseline_score,
    ),
    VariantShape(
        name="absent_clingen_allele_id",
        why="no ClinGen allele id, so the variant has no canonical IRI to report",
        kwargs={"clingen_allele_id": None},
    ),
    VariantShape(
        name="absent_gene_symbol",
        why="no mapped HGNC name, falling back to the target's own name",
        mutate=_drop_gene_symbol,
    ),
    VariantShape(
        name="non_coding_target",
        why="a regulatory target, whose identifier comes from post-mapped metadata",
        mutate=_make_non_coding,
    ),
    VariantShape(
        name="unmapped_hgvs_columns",
        why="a mapping carrying no hgvs_c, assay-level hgvs, or VEP consequence; CSV-only axis",
        kwargs={"hgvs_c": None, "hgvs_assay_level": None, "vep_functional_consequence": None},
    ),
]


def shape_ids() -> list[str]:
    """Shape names, for use as pytest parametrize ids."""
    return [shape.name for shape in VARIANT_SHAPES]
