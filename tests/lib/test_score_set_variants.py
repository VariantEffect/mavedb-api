# ruff: noqa: E402
"""Integration tests for the lean whole-set view assembly (``lib/score_set_variants.py``).

These pin the one-row-per-variant assembly: the submitted HGVS come off the variant, the mapped
assay-level HGVS off the live mapping record, the digest/ClinGen id/VEP consequence off the
authoritative allele; each HGVS string always rides even when it is not a placeable simple
substitution; unmapped variants are retained with null mapped fields; ``as_of`` reconstructs the
annotation layer while the immutable submitted HGVS and score stay put; and the set is ordered by
variant number.
"""

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.score_set_variants import HgvsField, MappedTriple, get_lean_score_set_variants
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from tests.helpers.constants import TEST_MINIMAL_VARIANT

# Deterministic windows far from the transaction clock (mirrors tests/lib/test_alleles.py).
T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2022, 1, 1, tzinfo=timezone.utc)


def _variant(session, score_set, suffix, *, score=None, hgvs_nt=None, hgvs_pro=None, hgvs_splice=None):
    data = {"score_data": {"score": score}} if score is not None else TEST_MINIMAL_VARIANT["data"]
    variant = Variant(
        urn=f"{score_set.urn}#{suffix}",
        score_set_id=score_set.id,
        data=data,
        hgvs_nt=hgvs_nt,
        hgvs_pro=hgvs_pro,
        hgvs_splice=hgvs_splice,
        creation_date=TEST_MINIMAL_VARIANT["creation_date"],
        modification_date=TEST_MINIMAL_VARIANT["modification_date"],
    )
    session.add(variant)
    session.commit()
    return variant


def _allele(session, digest, *, level="cdna", clingen_allele_id=None, hgvs_c=None, hgvs_g=None, hgvs_p=None):
    allele = Allele(
        vrs_digest=digest,
        level=level,
        post_mapped={"type": "Allele"},
        clingen_allele_id=clingen_allele_id,
        hgvs_c=hgvs_c,
        hgvs_g=hgvs_g,
        hgvs_p=hgvs_p,
    )
    session.add(allele)
    session.commit()
    return allele


def _record(session, variant, *, assay_level="cdna", hgvs_assay_level=None, valid_from=None):
    record = MappingRecord(
        variant_id=variant.id,
        assay_level=assay_level,
        hgvs_assay_level=hgvs_assay_level,
        mapping_api_version="test.0.0",
        valid_from=valid_from,
    )
    session.add(record)
    session.commit()
    return record


def _link(session, record, allele, *, is_authoritative=True, projection_group=None, valid_from=None):
    link = MappingRecordAllele(
        mapping_record_id=record.id,
        allele_id=allele.id,
        is_authoritative=is_authoritative,
        projection_group=projection_group,
        valid_from=valid_from,
    )
    session.add(link)
    session.commit()
    return link


def _consequence(session, allele, value):
    row = VepAlleleConsequence(
        allele_id=allele.id, functional_consequence=value, source_version="116", access_date="2026-01-01"
    )
    session.add(row)
    session.commit()
    return row


@pytest.mark.integration
def test_nucleotide_assay_measured_at_cdna(session, setup_lib_db_with_score_set):
    """Coding assay: submitted nt+pro from the variant; the mapped triple carries the measured cdna slot
    (from the record), its projection_group genomic sibling, and the protein apex; digest/ClinGen
    id/consequence off the authoritative allele. Each parseable string gets its block."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=-2.3, hgvs_nt="c.1A>T", hgvs_pro="p.Thr1Ser")
    record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.1216G>A")
    measured = _allele(session, "cdna-digest", level="cdna", clingen_allele_id="CA123")
    genomic = _allele(session, "gen-digest", level="genomic", hgvs_g="NC_000017.11:g.7676154C>T")
    protein = _allele(session, "prot-digest", level="protein", hgvs_p="NP_000537.3:p.Ala406Thr")
    # The measured cdna link and its genomic projection share a projection_group (the RT fold-in); the
    # protein apex is a member of no pair (group None).
    _link(session, record, measured, is_authoritative=True, projection_group=0)
    _link(session, record, genomic, is_authoritative=False, projection_group=0)
    _link(session, record, protein, is_authoritative=False)
    _consequence(session, measured, "missense_variant")

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.variant_urn == variant.urn
    assert record_out.score == -2.3
    assert record_out.consequence == "missense_variant"
    assert record_out.clingen_allele_id == "CA123"
    assert record_out.assay_level_digest == "cdna-digest"  # the measured (authoritative) allele
    assert record_out.hgvs_nt == HgvsField(hgvs="c.1A>T", position=1, ref="A", alt="T")
    assert record_out.hgvs_pro == HgvsField(hgvs="p.Thr1Ser", position=1, ref="Thr", alt="Ser")
    assert record_out.hgvs_splice is None
    # assayLevel names the measured slot; the triple fills all three levels.
    assert record_out.assay_level == "cdna"
    assert record_out.mapped == MappedTriple(
        cdna=HgvsField(hgvs="NM_000546.6:c.1216G>A", position=1216, ref="G", alt="A"),
        genomic=HgvsField(hgvs="NC_000017.11:g.7676154C>T", position=7676154, ref="C", alt="T"),
        protein=HgvsField(hgvs="NP_000537.3:p.Ala406Thr", position=406, ref="Ala", alt="Thr"),
    )


@pytest.mark.integration
def test_nucleotide_assay_measured_at_genomic(session, setup_lib_db_with_score_set):
    """Genomic assay: the measured slot is genomic (from the record); the cdna slot — the level-invariant
    search key — comes off the projection_group sibling even though the assay was measured at genomic."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=0.1, hgvs_nt="g.7676154C>T")
    record = _record(session, variant, assay_level="genomic", hgvs_assay_level="NC_000017.11:g.7676154C>T")
    measured = _allele(session, "gen-digest", level="genomic", clingen_allele_id="CA9")
    cdna = _allele(session, "cdna-digest", level="cdna", hgvs_c="NM_000546.6:c.1216G>A")
    protein = _allele(session, "prot-digest", level="protein", hgvs_p="NP_000537.3:p.Ala406Thr")
    _link(session, record, measured, is_authoritative=True, projection_group=0)
    _link(session, record, cdna, is_authoritative=False, projection_group=0)
    _link(session, record, protein, is_authoritative=False)

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.assay_level == "genomic"
    assert record_out.assay_level_digest == "gen-digest"
    # Measured slot is genomic (from the record); cdna (search key) comes off the sibling allele.
    assert record_out.mapped == MappedTriple(
        genomic=HgvsField(hgvs="NC_000017.11:g.7676154C>T", position=7676154, ref="C", alt="T"),
        cdna=HgvsField(hgvs="NM_000546.6:c.1216G>A", position=1216, ref="G", alt="A"),
        protein=HgvsField(hgvs="NP_000537.3:p.Ala406Thr", position=406, ref="Ala", alt="Thr"),
    )


@pytest.mark.integration
def test_protein_assay_serves_only_the_protein_slot(session, setup_lib_db_with_score_set):
    """Protein assay: the measured slot *is* protein, so only the protein slot is populated; the cdna and
    genomic slots stay null (the c/g fan-out is ambiguous — no canonical pick is fabricated)."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=1.0, hgvs_pro="p.Ala406Thr")
    record = _record(session, variant, assay_level="protein", hgvs_assay_level="NP_000537.3:p.Ala406Thr")
    measured = _allele(
        session, "prot-digest", level="protein", clingen_allele_id="PA9", hgvs_p="NP_000537.3:p.Ala406Thr"
    )
    _link(session, record, measured, is_authoritative=True)
    # A candidate fan-out member (cdna) with its own projection pair group — the protein authoritative link is
    # in no group, so this candidate must NOT be pulled into the canonical triple.
    candidate = _allele(session, "cand-digest", level="cdna", hgvs_c="NM_000546.6:c.1216G>A")
    _link(session, record, candidate, is_authoritative=False, projection_group=0)

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.assay_level == "protein"
    assert record_out.assay_level_digest == "prot-digest"
    assert record_out.clingen_allele_id == "PA9"
    # Only the protein slot; cdna/genomic stay null despite the candidate fan-out being linked.
    assert record_out.mapped == MappedTriple(
        protein=HgvsField(hgvs="NP_000537.3:p.Ala406Thr", position=406, ref="Ala", alt="Thr")
    )


@pytest.mark.integration
def test_unpopulated_projection_group_degrades_to_a_null_sibling(session, setup_lib_db_with_score_set):
    """Pre-reverse-translation data (no projection_group on the authoritative link): the sibling join
    returns nothing, so the other nucleotide slot stays null — today's behavior — while the measured and
    protein slots still populate. Still one row per variant."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=-2.3, hgvs_nt="c.1A>T")
    record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.1216G>A")
    measured = _allele(session, "cdna-digest", level="cdna", clingen_allele_id="CA123")
    protein = _allele(session, "prot-digest", level="protein", hgvs_p="NP_000537.3:p.Ala406Thr")
    _link(session, record, measured, is_authoritative=True)  # projection_group defaults to None
    _link(session, record, protein, is_authoritative=False)

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.assay_level == "cdna"
    # Measured cdna + protein apex populate; genomic is null (no recorded sibling — graceful degradation).
    assert record_out.mapped == MappedTriple(
        cdna=HgvsField(hgvs="NM_000546.6:c.1216G>A", position=1216, ref="G", alt="A"),
        protein=HgvsField(hgvs="NP_000537.3:p.Ala406Thr", position=406, ref="Ala", alt="Thr"),
    )


@pytest.mark.integration
def test_hgvs_string_rides_even_when_unparseable(session, setup_lib_db_with_score_set):
    """The canonical string is always present; a splice/multivariant expression simply carries no block."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, hgvs_nt="NM_000546.6:c.[197A>G;472T>C]", hgvs_splice="c.122-6T>A")
    record = _record(session, variant, assay_level="cdna", hgvs_assay_level="c.76_78del")
    measured = _allele(session, "cdna-digest", level="cdna")
    _link(session, record, measured, is_authoritative=True)

    [record_out] = get_lean_score_set_variants(session, score_set)

    # Multivariant, intronic, and indel expressions are all string-only (no position/ref/alt block).
    assert record_out.hgvs_nt == HgvsField(hgvs="NM_000546.6:c.[197A>G;472T>C]")
    assert record_out.hgvs_nt.position is None
    assert record_out.hgvs_splice == HgvsField(hgvs="c.122-6T>A")
    # The measured cdna slot carries the (unparseable) indel string alone.
    assert record_out.mapped.cdna == HgvsField(hgvs="c.76_78del")


@pytest.mark.integration
def test_unmapped_variant_has_null_mapped_fields(session, setup_lib_db_with_score_set):
    """A variant with no live mapping record keeps its submitted HGVS + score but null mapped fields."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=0.5, hgvs_nt="c.1A>T", hgvs_pro="p.Thr1Ser")

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.variant_urn == variant.urn
    assert record_out.score == 0.5
    assert record_out.hgvs_nt == HgvsField(hgvs="c.1A>T", position=1, ref="A", alt="T")
    assert record_out.consequence is None
    assert record_out.clingen_allele_id is None
    assert record_out.assay_level_digest is None
    assert record_out.assay_level is None
    assert record_out.mapped == MappedTriple()  # empty triple — no slots populated


@pytest.mark.integration
def test_no_vep_consequence_is_none(session, setup_lib_db_with_score_set):
    """A mapped variant with no live VEP consequence has consequence None but keeps its digest + mapped HGVS."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, hgvs_nt="c.1A>T")
    record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.1216G>A")
    measured = _allele(session, "cdna-digest", level="cdna")
    _link(session, record, measured, is_authoritative=True)

    [record_out] = get_lean_score_set_variants(session, score_set)

    assert record_out.consequence is None
    assert record_out.assay_level_digest == "cdna-digest"
    assert record_out.mapped.cdna == HgvsField(hgvs="NM_000546.6:c.1216G>A", position=1216, ref="G", alt="A")


@pytest.mark.integration
def test_ordered_by_variant_number(session, setup_lib_db_with_score_set):
    """Records come back ordered by the integer after '#', not insertion or lexical order."""
    score_set = setup_lib_db_with_score_set
    for suffix in (10, 2, 1):
        _variant(session, score_set, suffix, score=float(suffix))

    records = get_lean_score_set_variants(session, score_set)

    assert [r.variant_urn for r in records] == [f"{score_set.urn}#{n}" for n in (1, 2, 10)]


@pytest.mark.integration
def test_as_of_reconstructs_the_historical_annotation_layer(session, setup_lib_db_with_score_set):
    """A re-map retires the old record and inserts a new one; as_of reconstructs the mapped layer live at
    a past instant, while the immutable submitted HGVS and score are unaffected."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, score=-2.3, hgvs_nt="c.1A>T")

    old_record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.1216G>A", valid_from=T0)
    old_allele = _allele(session, "old-digest", level="cdna")
    _link(session, old_record, old_allele, is_authoritative=True, valid_from=T0)
    old_record.retire(session, at=T1)  # cascades to the link via __retire_cascade__

    new_record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.500A>T", valid_from=T1)
    new_allele = _allele(session, "new-digest", level="cdna")
    _link(session, new_record, new_allele, is_authoritative=True, valid_from=T1)
    session.commit()

    [current] = get_lean_score_set_variants(session, score_set)
    assert current.assay_level_digest == "new-digest"
    assert current.mapped.cdna == HgvsField(hgvs="NM_000546.6:c.500A>T", position=500, ref="A", alt="T")

    [historical] = get_lean_score_set_variants(session, score_set, as_of=T1 - timedelta(days=1))
    assert historical.assay_level_digest == "old-digest"
    assert historical.mapped.cdna == HgvsField(hgvs="NM_000546.6:c.1216G>A", position=1216, ref="G", alt="A")
    # Submitted HGVS and score are immutable — unaffected by as_of.
    assert historical.hgvs_nt == HgvsField(hgvs="c.1A>T", position=1, ref="A", alt="T")
    assert historical.score == -2.3

    # Before anything was mapped, the variant still appears with null mapped fields.
    [pre_mapping] = get_lean_score_set_variants(session, score_set, as_of=T0 - timedelta(days=1))
    assert pre_mapping.assay_level_digest is None
    assert pre_mapping.assay_level is None
    assert pre_mapping.mapped == MappedTriple()
