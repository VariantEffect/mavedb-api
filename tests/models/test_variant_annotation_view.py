# ruff: noqa: E402
"""Tests for the v_variant_annotations view after its rewrite onto the MappingRecord/Allele substrate.

The view is a flat operator convenience projection: one row per (variant, current annotation_type). It
reconstructs the legacy ``mapped_variants`` row's flat ``hgvs_g``/``hgvs_c``/``hgvs_p`` triple by
pivoting the record's live allele links by level, and carries the single-valued ``clingen_allele_id``
and VEP consequence from the record's authoritative allele.
"""

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_view import VariantAnnotationView
from tests.helpers.constants import TEST_MINIMAL_VARIANT
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record


def _rows_for(session, variant_urn):
    # Select explicit columns (not the whole entity): the view is keyless, so an all-null-identity row
    # (an unmapped variant) would materialize as a single ``None`` entity. Column rows sidestep that.
    stmt = select(
        VariantAnnotationView.mapping_record_id,
        VariantAnnotationView.hgvs_assay_level,
        VariantAnnotationView.hgvs_g,
        VariantAnnotationView.hgvs_c,
        VariantAnnotationView.hgvs_p,
        VariantAnnotationView.clingen_allele_id,
        VariantAnnotationView.vep_functional_consequence,
    ).where(VariantAnnotationView.variant_urn == variant_urn)
    return list(session.execute(stmt).all())


def test_reconstructs_flat_triple_and_authoritative_fields(session, setup_lib_db_with_variant):
    """A cdna-assay record with genomic/cdna/protein alleles fills all three HGVS slots; the
    authoritative (cdna) allele supplies clingen_allele_id and the VEP consequence."""
    variant = setup_lib_db_with_variant
    record = seed_mapping_record(
        session,
        variant,
        assay_level="cdna",
        hgvs_assay_level="NM_000001.1:c.1A>T",
        alleles=[
            AlleleSpec(
                digest="d-cdna",
                level="cdna",
                is_authoritative=True,
                clingen_allele_id="CA123",
                hgvs_c="NM_000001.1:c.1A>T",
                vep_consequence="missense_variant",
            ),
            AlleleSpec(digest="d-genomic", level="genomic", hgvs_g="NC_000001.11:g.100A>T"),
            AlleleSpec(digest="d-protein", level="protein", hgvs_p="NP_000001.1:p.Met1Leu"),
        ],
    )

    rows = _rows_for(session, variant.urn)
    assert len(rows) == 1
    row = rows[0]
    assert row.mapping_record_id == record.id
    assert row.hgvs_assay_level == "NM_000001.1:c.1A>T"
    assert row.hgvs_g == "NC_000001.11:g.100A>T"
    assert row.hgvs_c == "NM_000001.1:c.1A>T"
    assert row.hgvs_p == "NP_000001.1:p.Met1Leu"
    assert row.clingen_allele_id == "CA123"
    assert row.vep_functional_consequence == "missense_variant"


def test_protein_assay_leaves_nucleotide_slots_null(session, setup_lib_db_with_variant):
    """A protein-assay record has only a protein allele: hgvs_p is filled, the nucleotide slots stay
    null, and the authoritative protein allele still supplies clingen_allele_id."""
    variant = setup_lib_db_with_variant
    seed_mapping_record(
        session,
        variant,
        assay_level="protein",
        hgvs_assay_level="NP_000001.1:p.Met1Leu",
        alleles=[
            AlleleSpec(
                digest="d-prot-only",
                level="protein",
                is_authoritative=True,
                clingen_allele_id="PA9",
                hgvs_p="NP_000001.1:p.Met1Leu",
            ),
        ],
    )

    row = _rows_for(session, variant.urn)[0]
    assert row.hgvs_p == "NP_000001.1:p.Met1Leu"
    assert row.hgvs_g is None
    assert row.hgvs_c is None
    assert row.clingen_allele_id == "PA9"


def test_unmapped_variant_retained_with_null_mapping(session, setup_lib_db_with_score_set):
    """The outer join keeps a variant with no live mapping record — it still appears with a null
    mapping_record_id and empty mapped fields."""
    variant = Variant(
        **TEST_MINIMAL_VARIANT, urn=f"{setup_lib_db_with_score_set.urn}#7", score_set_id=setup_lib_db_with_score_set.id
    )
    session.add(variant)
    session.commit()
    session.refresh(variant)

    row = _rows_for(session, variant.urn)[0]
    assert row.mapping_record_id is None
    assert row.hgvs_g is None
    assert row.hgvs_c is None
    assert row.hgvs_p is None
    assert row.clingen_allele_id is None
    assert row.vep_functional_consequence is None
