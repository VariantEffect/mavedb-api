# ruff: noqa: E402
"""Integration tests for the variant-detail envelope assembly (``lib/variant_detail.py``).

These pin the composition: the flat assay fields (level, target/reference HGVS, digest, ClinGen id)
off the live record + authoritative allele; the spec-pure Cat-VRS + MaveDB mode/member-relations off
the on-the-fly builder; the digest-keyed annotation map; the per-calibration classifications
(primary first, filterable by visibility); scores/counts passthrough; and the ``is_current`` /
``superseded_by_score_set`` version standing. Cat-VRS/annotation internals are covered in their own suites — here
we assert they are wired in and keyed correctly.
"""

from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.variant_detail import get_variant_detail
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from tests.helpers.constants import TEST_MINIMAL_VARIANT, TEST_USER

# A spec-valid post_mapped VRS Allele — the Cat-VRS builder hydrates this, so it must parse.
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


def _variant(session, score_set, suffix, *, data=None, hgvs_nt=None, hgvs_pro=None):
    variant = Variant(
        urn=f"{score_set.urn}#{suffix}",
        score_set_id=score_set.id,
        data=data if data is not None else TEST_MINIMAL_VARIANT["data"],
        hgvs_nt=hgvs_nt,
        hgvs_pro=hgvs_pro,
        creation_date=TEST_MINIMAL_VARIANT["creation_date"],
        modification_date=TEST_MINIMAL_VARIANT["modification_date"],
    )
    session.add(variant)
    session.commit()
    return variant


def _allele(session, digest, *, level="cdna", clingen_allele_id=None, hgvs_g=None, hgvs_c=None, hgvs_p=None):
    allele = Allele(
        vrs_digest=digest,
        level=level,
        post_mapped=_post_mapped(),
        clingen_allele_id=clingen_allele_id,
        hgvs_g=hgvs_g,
        hgvs_c=hgvs_c,
        hgvs_p=hgvs_p,
    )
    session.add(allele)
    session.commit()
    return allele


def _record(session, variant, *, assay_level="cdna", hgvs_assay_level=None):
    record = MappingRecord(
        variant_id=variant.id,
        assay_level=assay_level,
        hgvs_assay_level=hgvs_assay_level,
        mapping_api_version="test.0.0",
    )
    session.add(record)
    session.commit()
    return record


def _link(session, record, allele, *, is_authoritative=False):
    link = MappingRecordAllele(mapping_record_id=record.id, allele_id=allele.id, is_authoritative=is_authoritative)
    session.add(link)
    session.commit()
    return link


def _vep(session, allele, value):
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id, functional_consequence=value, source_version="116", access_date="2026-01-01"
        )
    )
    session.commit()


def _calibration(session, score_set, *, primary, classifications):
    """A calibration with functional classifications; each ``classifications`` entry is
    ``(functional, [variants...])`` and its m2m membership is set to those variants."""
    user = session.query(User).filter(User.username == TEST_USER["username"]).one()
    calibration = ScoreCalibration(
        score_set_id=score_set.id,
        title=f"cal-{'primary' if primary else 'secondary'}",
        primary=primary,
        private=False,
        created_by_id=user.id,
        modified_by_id=user.id,
    )
    session.add(calibration)
    session.commit()
    for functional, variants in classifications:
        fc = ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id, label=functional, functional_classification=functional
        )
        fc.variants = variants
        session.add(fc)
    session.commit()
    return calibration


@pytest.mark.integration
def test_full_envelope_for_a_coding_assay(session, setup_lib_db_with_score_set):
    """Mode 1 (coding measured): flat assay fields off the record + authoritative allele, spec-pure
    Cat-VRS in projection mode, and a digest-keyed annotation map covering the linked alleles."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(
        session, score_set, 1, data={"score_data": {"score": -2.3}}, hgvs_nt="c.1216G>A", hgvs_pro="p.Ala406Thr"
    )
    record = _record(session, variant, assay_level="cdna", hgvs_assay_level="NM_000546.6:c.1216G>A")
    measured = _allele(session, "cdna-digest", level="cdna", clingen_allele_id="CA123", hgvs_c="NM_000546.6:c.1216G>A")
    protein = _allele(session, "prot-digest", level="protein", hgvs_p="NP_000537.3:p.Ala406Thr")
    _link(session, record, measured, is_authoritative=True)
    _link(session, record, protein)
    _vep(session, measured, "missense_variant")

    detail = get_variant_detail(session, variant)

    assert detail.urn == variant.urn
    assert detail.scores == {"score": -2.3}
    assert detail.assay_level == "cdna"
    assert detail.target_hgvs == "c.1216G>A"  # submitted, target frame
    assert detail.reference_hgvs == "NM_000546.6:c.1216G>A"  # mapped, reference frame
    assert detail.assay_level_digest == "cdna-digest"
    assert detail.clingen_allele_id == "CA123"
    assert detail.mode == "projection"
    assert detail.molecular_representation is not None
    assert detail.molecular_representation["type"] == "CategoricalVariant"
    # The alleles sidecar carries one identity per linked allele, keyed by digest: level +
    # reference-frame HGVS (coalesced from hgvs_g/c/p) + ClinGen id + member->defining relation.
    assert set(detail.alleles) == {"cdna-digest", "prot-digest"}
    measured_identity = detail.alleles["cdna-digest"]
    assert measured_identity.level == "cdna"
    assert measured_identity.hgvs == "NM_000546.6:c.1216G>A"  # coalesced from hgvs_c
    assert measured_identity.clingen_allele_id == "CA123"
    assert measured_identity.relation is None  # the defining allele has no relation to itself
    protein_identity = detail.alleles["prot-digest"]
    assert protein_identity.level == "protein"
    assert protein_identity.hgvs == "NP_000537.3:p.Ala406Thr"  # coalesced from hgvs_p
    # The protein member is a translation of the measured coding allele (member -> defining).
    assert protein_identity.relation == "translation_of"
    # Annotations keyed by digest, covering both linked alleles; VEP rode in on the measured allele.
    assert set(detail.annotations) == {"cdna-digest", "prot-digest"}
    assert detail.annotations["cdna-digest"].vep is not None
    assert detail.annotations["cdna-digest"].vep.consequence == "missense_variant"
    assert detail.is_current is True
    assert detail.superseded_by_score_set is None


@pytest.mark.integration
def test_protein_assay_targets_the_protein_frame(session, setup_lib_db_with_score_set):
    """Mode 2 (protein measured): assay level is protein, target HGVS is the submitted p. string, and
    the nt member `encodes` the defining protein allele."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, hgvs_nt="c.1216G>A", hgvs_pro="p.Ala406Thr")
    record = _record(session, variant, assay_level="protein", hgvs_assay_level="NP_000537.3:p.Ala406Thr")
    measured = _allele(session, "prot-digest", level="protein", clingen_allele_id="PA9")
    coding = _allele(session, "cdna-digest", level="cdna")
    _link(session, record, measured, is_authoritative=True)
    _link(session, record, coding)

    detail = get_variant_detail(session, variant)

    assert detail.assay_level == "protein"
    assert detail.target_hgvs == "p.Ala406Thr"  # protein assay -> submitted protein string
    assert detail.reference_hgvs == "NP_000537.3:p.Ala406Thr"
    assert detail.assay_level_digest == "prot-digest"
    assert detail.mode == "reverse_translation"
    # Defining protein allele has no relation to itself; the coding member encodes it.
    assert detail.alleles["prot-digest"].level == "protein"
    assert detail.alleles["prot-digest"].relation is None
    assert detail.alleles["cdna-digest"].level == "cdna"
    assert detail.alleles["cdna-digest"].relation == "encodes"


@pytest.mark.integration
def test_unmapped_variant_has_null_molecular_layer(session, setup_lib_db_with_score_set):
    """No live record: flat mapped fields and Cat-VRS are null, the annotation map is empty, and the
    variant reads as current."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 0.5}}, hgvs_nt="c.1A>T")

    detail = get_variant_detail(session, variant)

    assert detail.scores == {"score": 0.5}
    assert detail.assay_level is None
    assert detail.target_hgvs == "c.1A>T"  # submitted still surfaces (no assay level -> nucleotide)
    assert detail.reference_hgvs is None
    assert detail.assay_level_digest is None
    assert detail.clingen_allele_id is None
    assert detail.molecular_representation is None
    assert detail.mode is None
    assert detail.alleles == {}
    assert detail.annotations == {}
    assert detail.is_current is True


@pytest.mark.integration
def test_classifications_are_per_calibration_primary_first(session, setup_lib_db_with_score_set):
    """A variant carries one classification per calibration that classifies it; the primary sorts first."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 0.8}})
    _calibration(session, score_set, primary=False, classifications=[("normal", [variant])])
    _calibration(session, score_set, primary=True, classifications=[("abnormal", [variant])])

    detail = get_variant_detail(session, variant)

    assert [(c.primary, c.classification.functional_classification.value) for c in detail.classifications] == [
        (True, "abnormal"),
        (False, "normal"),
    ]


@pytest.mark.integration
def test_visible_calibration_ids_filters_classifications(session, setup_lib_db_with_score_set):
    """Only classifications from calibrations the caller resolved as visible are returned."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 0.8}})
    visible = _calibration(session, score_set, primary=True, classifications=[("abnormal", [variant])])
    _calibration(session, score_set, primary=False, classifications=[("normal", [variant])])

    detail = get_variant_detail(session, variant, visible_calibration_ids={visible.id})

    assert [c.calibration_id for c in detail.classifications] == [visible.id]


@pytest.mark.integration
def test_superseded_variant_self_describes(session, setup_lib_db_with_score_set):
    """When a superseding version is passed in, the variant reports it rather than reading as current."""
    score_set = setup_lib_db_with_score_set
    variant = _variant(session, score_set, 1)
    newer = SimpleNamespace(urn="urn:mavedb:00000001-a-2")  # only .urn is read

    detail = get_variant_detail(session, variant, superseding_score_set=newer)

    assert detail.is_current is False
    assert detail.superseded_by_score_set == "urn:mavedb:00000001-a-2"
