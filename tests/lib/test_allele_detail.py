# ruff: noqa: E402
"""Integration tests for the allele-detail assembly backing ``GET /alleles/{digest}`` (+ CAID).

``get_allele_detail`` is the allele-grain sibling of ``get_variant_detail``: it anchors on an allele
(a digest, or a CAID's genomic+coding pair) and serves the **full cross-layer equivalence class**
(``get_allele_translations``), with each member labelled *relative to the focus* — faithful
(``projection``) vs. convergent cousin vs. reverse-translation ``candidate`` — plus the digest-keyed
annotations. These tests pin the labelling in both directions (nt focus / protein focus), the full-class
membership incl. cousins, the CAID focus set, the orphan fallback, and ``as_of``.
"""

from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.lib.allele_detail import get_allele_detail
from mavedb.models.allele import Allele
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_MINIMAL_VARIANT
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record

T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2022, 1, 1, tzinfo=timezone.utc)

_VALID_DIGEST = "0123456789abcdefghijABCDEFGHIJ_-"


def _post_mapped() -> dict:
    return {
        "id": f"ga4gh:VA.{_VALID_DIGEST}",
        "type": "Allele",
        "state": {"type": "LiteralSequenceExpression", "sequence": "F"},
        "digest": _VALID_DIGEST,
        "location": {
            "id": f"ga4gh:SL.{_VALID_DIGEST}",
            "end": 6,
            "type": "SequenceLocation",
            "start": 5,
            "digest": _VALID_DIGEST,
            "sequenceReference": {
                "type": "SequenceReference",
                "label": "NP_000000.0",
                "refgetAccession": "SQ.0123456789abcdefghijABCDEFGHIJ_-",
            },
        },
    }


def _variant(session, score_set, suffix):
    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#{suffix}", score_set_id=score_set.id)
    session.add(variant)
    session.commit()
    return variant


def _allele(session, digest) -> Allele:
    allele = session.scalar(select(Allele).where(Allele.vrs_digest == digest))
    assert allele is not None, f"allele {digest!r} not seeded"
    return allele


def _coding_measured_specs():
    """A coding-measured record: authoritative cdna (+VEP+ClinGen), its genomic projection partner
    (shared projection_group 0), the protein apex, and a synonymous cousin (own projection_group 1)."""
    return [
        AlleleSpec(
            digest="cdna-d",
            level="cdna",
            is_authoritative=True,
            clingen_allele_id="CA1",
            hgvs_c="NM_000546.6:c.1216G>A",
            post_mapped=_post_mapped(),
            vep_consequence="missense_variant",
            projection_group=0,
        ),
        AlleleSpec(
            digest="gen-d",
            level="genomic",
            clingen_allele_id="CA1",
            hgvs_g="NC_000017.11:g.7676154C>T",
            post_mapped=_post_mapped(),
            projection_group=0,
        ),
        AlleleSpec(digest="prot-d", level="protein", hgvs_p="NP_000537.3:p.Ala406Thr", post_mapped=_post_mapped()),
        AlleleSpec(
            digest="cousin-d",
            level="cdna",
            hgvs_c="NM_000546.6:c.1218C>T",
            post_mapped=_post_mapped(),
            projection_group=1,
        ),
    ]


@pytest.mark.integration
def test_nt_focus_labels_the_full_class_relative_to_the_queried_allele(session, setup_lib_db_with_score_set):
    """Fetching the measured coding allele: the whole class comes back, each member labelled relative to
    it — its genomic is a faithful coordinate projection, the protein its consequence, the cousin convergent."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    seed_mapping_record(session, variant, alleles=_coding_measured_specs(), assay_level="cdna")

    detail = get_allele_detail(session, _allele(session, "cdna-d"), focus_digests={"cdna-d"})

    assert detail.digest == "cdna-d"
    assert detail.level == "cdna"
    assert detail.vrs is not None and detail.vrs["type"] == "Allele"
    assert set(detail.alleles) == {"cdna-d", "gen-d", "prot-d", "cousin-d"}  # full class, cousin included

    focus = detail.alleles["cdna-d"]
    assert focus.is_focus is True and focus.relation is None and focus.derivation is None
    assert focus.projection_of == "gen-d"

    genomic = detail.alleles["gen-d"]
    assert genomic.is_focus is False
    assert genomic.relation == "coordinate_representation_of"
    assert genomic.derivation == "projection"
    assert genomic.projection_of == "cdna-d"

    protein = detail.alleles["prot-d"]
    assert protein.relation == "translation_of"
    assert protein.derivation == "projection"

    cousin = detail.alleles["cousin-d"]
    assert cousin.relation == "co_encodes"
    assert cousin.derivation == "convergent"  # distinct change sharing the consequence, not the coordinate partner


@pytest.mark.integration
def test_protein_focus_labels_every_nucleotide_as_a_candidate(session, setup_lib_db_with_score_set):
    """Fetching the protein apex: walking down is ambiguous, so every nt member is a reverse-translation
    candidate (the 'less power without a measurement' case, still labelled precisely by direction)."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    seed_mapping_record(session, variant, alleles=_coding_measured_specs(), assay_level="cdna")

    detail = get_allele_detail(session, _allele(session, "prot-d"), focus_digests={"prot-d"})

    assert detail.alleles["prot-d"].is_focus is True
    for nt in ("cdna-d", "gen-d", "cousin-d"):
        assert detail.alleles[nt].relation == "encodes", nt
        assert detail.alleles[nt].derivation == "candidate", nt


@pytest.mark.integration
def test_annotations_join_by_digest(session, setup_lib_db_with_score_set):
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    seed_mapping_record(session, variant, alleles=_coding_measured_specs(), assay_level="cdna")

    detail = get_allele_detail(session, _allele(session, "cdna-d"), focus_digests={"cdna-d"})

    assert set(detail.annotations) == set(detail.alleles)
    assert detail.annotations["cdna-d"].vep is not None
    assert detail.annotations["cdna-d"].vep.consequence == "missense_variant"
    assert detail.annotations["gen-d"].vep is None  # a member with no annotations still gets an empty block


@pytest.mark.integration
def test_caid_focus_flags_both_frames(session, setup_lib_db_with_score_set):
    """A CAID names the nt change in both frames: fetched by CAID, the genomic + coding are both isFocus
    (each other's coordinate partner), and only the true cousin is convergent."""
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    seed_mapping_record(session, variant, alleles=_coding_measured_specs(), assay_level="cdna")

    # The CAID resolves to the {cdna, genomic} pair (both carry clingen_allele_id="CA1").
    detail = get_allele_detail(session, _allele(session, "cdna-d"), focus_digests={"cdna-d", "gen-d"})

    assert detail.alleles["cdna-d"].is_focus is True
    assert detail.alleles["gen-d"].is_focus is True
    assert detail.alleles["prot-d"].relation == "translation_of"
    assert detail.alleles["cousin-d"].derivation == "convergent"


@pytest.mark.integration
def test_orphan_allele_falls_back_to_itself(session, setup_lib_db_with_score_set):
    orphan = Allele(vrs_digest="orphan-d", level="cdna", hgvs_c="NM:c.9A>G", post_mapped=_post_mapped())
    session.add(orphan)
    session.commit()

    detail = get_allele_detail(session, orphan, focus_digests={"orphan-d"})

    assert set(detail.alleles) == {"orphan-d"}
    assert detail.alleles["orphan-d"].is_focus is True
    assert set(detail.annotations) == {"orphan-d"}


@pytest.mark.integration
def test_as_of_reconstructs_membership(session, setup_lib_db_with_score_set):
    variant = _variant(session, setup_lib_db_with_score_set, 1)
    seed_mapping_record(session, variant, alleles=_coding_measured_specs(), assay_level="cdna", valid_from=T1)

    anchor = _allele(session, "cdna-d")

    before = get_allele_detail(session, anchor, focus_digests={"cdna-d"}, as_of=T0)
    assert set(before.alleles) == {"cdna-d"}  # links not yet live → class collapses to the focus
    assert before.annotations["cdna-d"].vep is None

    after = get_allele_detail(session, anchor, focus_digests={"cdna-d"}, as_of=T2)
    assert set(after.alleles) == {"cdna-d", "gen-d", "prot-d", "cousin-d"}
    assert after.annotations["cdna-d"].vep is not None
