# ruff: noqa: E402

"""Fixtures scoped to the CSV library tests."""

import pytest

pytest.importorskip("psycopg2")

from tests.helpers.constants import TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record


@pytest.fixture
def setup_lib_db_with_mapped_variant(session, setup_lib_db_with_variant, setup_lib_db_with_mapped_variant):
    """Give the shared fixture's variant a live mapping on the allele substrate as well.

    The CSV surfaces resolve their mapping-derived namespaces from the allele graph, not from the frozen
    ``MappedVariant`` row the parent fixture seeds, so without this every such column comes back NA.
    Overridden here rather than in the parent conftest because tests elsewhere under ``tests/lib`` seed
    their own records, and a second live record per variant violates ``uq_mapping_records_current``.

    Seeded as the full g/c/p triple the CSV reconstructs from — an authoritative genomic allele, its
    projection-group cdna sibling, and the protein apex — so a test can populate any level's HGVS by
    setting it on that level's allele. Mirrors ``tests.helpers.util.score_set.seed_csv_substrate``.
    """
    seed_mapping_record(
        session,
        setup_lib_db_with_variant,
        assay_level="genomic",
        hgvs_assay_level=TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION["hgvs_g"],
        alleles=[
            AlleleSpec(
                digest="lib-csv-genomic",
                level="genomic",
                is_authoritative=True,
                projection_group=0,
                hgvs_g=TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION["hgvs_g"],
                vep_consequence=TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION["vep_functional_consequence"],
            ),
            AlleleSpec(digest="lib-csv-cdna", level="cdna", projection_group=0),
            AlleleSpec(digest="lib-csv-protein", level="protein"),
        ],
    )
    session.refresh(setup_lib_db_with_mapped_variant)
    return setup_lib_db_with_mapped_variant
