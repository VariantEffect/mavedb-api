# ruff: noqa: E402
"""Integration tests for the ClinGen-allele measurements list (``lib/allele_measurements.py``).

These pin the model that the first pass got wrong: measurements aggregate the **cross-layer equivalence
class** (co-membership in the mapping/RT link graph), not one exact allele, and *which* allele anchors it
determines the direct/related split (a CA anchors on the nt alleles → nt measurements are direct, the
protein consequence is related; a PA anchors on the protein → protein measurements are direct, the nt
encodings related). Also: the narrowness (a sibling nt change is not pulled onto a CA page), the two
authorization gates (score-set READ hides the measurement; calibration READ withholds only the inline
classification) exercised through the real ``has_permission``, superseded opt-in, and the default ordering
(direct-first, strongest evidence, pathogenic-biased).
"""

import pytest

pytest.importorskip("psycopg2")

from datetime import date

from mavedb.lib.allele_measurements import (
    AlleleMeasurement,
    MeasurementRelationship,
    _ordering_key,
    get_allele_measurements,
)
from mavedb.lib.types.authentication import UserData
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from tests.helpers.constants import EXTRA_USER, TEST_MINIMAL_VARIANT, TEST_SEQ_SCORESET, TEST_USER


def _user(session, username):
    return session.query(User).filter(User.username == username).one()


def _user_data(session, username=TEST_USER["username"]):
    """A ``UserData`` for a seeded user — the currency ``has_permission`` reads. ``None`` is anonymous."""
    return UserData(user=_user(session, username), active_roles=[])


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


def _allele(session, digest, *, level="cdna", clingen_allele_id=None):
    allele = Allele(vrs_digest=digest, level=level, clingen_allele_id=clingen_allele_id)
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
    session.add(
        MappingRecordAllele(mapping_record_id=record.id, allele_id=allele.id, is_authoritative=is_authoritative)
    )
    session.commit()


def _calibration(
    session,
    score_set,
    *,
    variants,
    functional="abnormal",
    oddspaths_ratio=None,
    primary=True,
    private=False,
    investigator_provided=False,
    research_use_only=False,
):
    user = _user(session, TEST_USER["username"])
    calibration = ScoreCalibration(
        score_set_id=score_set.id,
        title="cal",
        primary=primary,
        private=private,
        investigator_provided=investigator_provided,
        research_use_only=research_use_only,
        created_by_id=user.id,
        modified_by_id=user.id,
    )
    session.add(calibration)
    session.commit()
    fc = ScoreCalibrationFunctionalClassification(
        calibration_id=calibration.id,
        label=functional,
        functional_classification=functional,
        oddspaths_ratio=oddspaths_ratio,
    )
    fc.variants = variants
    session.add(fc)
    session.commit()
    return calibration


def _second_score_set(
    session, base, urn, *, owner_username=TEST_USER["username"], private=True, superseded_score_set_id=None
):
    """A sibling score set in the same experiment — for cross-score-set, private, and supersession tests.
    ``superseded_score_set_id`` set on this (newer) row makes it the superseding version of that older
    set (``older.superseding_score_set`` resolves back here)."""
    owner = _user(session, owner_username)
    scaffold = TEST_SEQ_SCORESET.copy()
    scaffold.pop("target_genes", None)
    score_set = ScoreSet(
        **scaffold,
        urn=urn,
        experiment_id=base.experiment_id,
        licence_id=base.licence_id,
        superseded_score_set_id=superseded_score_set_id,
    )
    score_set.private = private
    score_set.created_by = owner
    score_set.modified_by = owner
    session.add(score_set)
    session.commit()
    session.refresh(score_set)
    return score_set


def _seed_two_level_pair(session, score_set):
    """One nt change (CA123) and its protein consequence (PA9), each measured in its own assay, with the
    other level riding as a member — the minimal shape that exercises the direct/related asymmetry."""
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    prot = _allele(session, "prot-P", level="protein", clingen_allele_id="PA9")

    nt_measured = _variant(session, score_set, 1, data={"score_data": {"score": -2.0}}, hgvs_nt="c.1A>T")
    nt_record = _record(session, nt_measured, assay_level="cdna", hgvs_assay_level="NM_0:c.1A>T")
    _link(session, nt_record, nt, is_authoritative=True)
    _link(session, nt_record, prot)

    protein_measured = _variant(session, score_set, 2, data={"score_data": {"score": 1.0}}, hgvs_pro="p.Met1Leu")
    protein_record = _record(session, protein_measured, assay_level="protein", hgvs_assay_level="NP_0:p.Met1Leu")
    _link(session, protein_record, prot, is_authoritative=True)
    _link(session, protein_record, nt)

    return nt_measured, protein_measured


@pytest.mark.integration
def test_ca_entry_aggregates_direct_and_related(session, setup_lib_db_with_score_set):
    """A CA page: the nt measurement is direct (assayed at this change); the protein measurement of its
    consequence is related, labeled ``protein_consequence``, each carrying its own measured level."""
    score_set = setup_lib_db_with_score_set
    nt_measured, protein_measured = _seed_two_level_pair(session, score_set)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))

    by_urn = {m.variant_urn: m for m in result}
    assert set(by_urn) == {nt_measured.urn, protein_measured.urn}
    assert (by_urn[nt_measured.urn].relationship, by_urn[nt_measured.urn].assay_level) == ("direct", "cdna")
    assert (by_urn[protein_measured.urn].relationship, by_urn[protein_measured.urn].assay_level) == (
        "protein_consequence",
        "protein",
    )
    # Assay HGVS pair rides through (mapped reference + submitted target).
    assert by_urn[nt_measured.urn].assay_level_hgvs == "NM_0:c.1A>T"
    assert by_urn[nt_measured.urn].submitted_hgvs == "c.1A>T"
    assert by_urn[protein_measured.urn].submitted_hgvs == "p.Met1Leu"


@pytest.mark.integration
def test_pa_entry_swaps_direct_and_related(session, setup_lib_db_with_score_set):
    """The same data anchored on the PA: the protein measurement is now direct and the nt measurement is
    the related ``nucleotide_encoding`` — the asymmetry is purely which allele anchors the co-membership."""
    score_set = setup_lib_db_with_score_set
    nt_measured, protein_measured = _seed_two_level_pair(session, score_set)

    result = get_allele_measurements(session, "PA9", user_data=_user_data(session))

    by_urn = {m.variant_urn: m for m in result}
    assert set(by_urn) == {nt_measured.urn, protein_measured.urn}
    assert by_urn[protein_measured.urn].relationship == "direct"
    assert by_urn[nt_measured.urn].relationship == "nucleotide_encoding"


@pytest.mark.integration
def test_sibling_nt_not_pulled_onto_ca_page(session, setup_lib_db_with_score_set):
    """A sibling nt change that encodes the *same* protein is not pulled onto a CA page (its record links
    the protein but not this nt allele) — but both nt changes show on the protein's PA page as encodings."""
    score_set = setup_lib_db_with_score_set
    nt1 = _allele(session, "nt-1", level="cdna", clingen_allele_id="CA111")
    nt2 = _allele(session, "nt-2", level="cdna", clingen_allele_id="CA222")
    prot = _allele(session, "prot-P", level="protein", clingen_allele_id="PA9")

    a = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    ra = _record(session, a, assay_level="cdna")
    _link(session, ra, nt1, is_authoritative=True)
    _link(session, ra, prot)

    c = _variant(session, score_set, 2, data={"score_data": {"score": 1.0}})
    rc = _record(session, c, assay_level="cdna")
    _link(session, rc, nt2, is_authoritative=True)
    _link(session, rc, prot)

    ca_page = get_allele_measurements(session, "CA111", user_data=_user_data(session))
    assert {m.variant_urn for m in ca_page} == {a.urn}

    pa_page = get_allele_measurements(session, "PA9", user_data=_user_data(session))
    assert {m.variant_urn for m in pa_page} == {a.urn, c.urn}
    assert all(m.relationship == "nucleotide_encoding" for m in pa_page)


@pytest.mark.integration
def test_sibling_nt_pulled_onto_ca_page_with_flag(session, setup_lib_db_with_score_set):
    """``include_nucleotide_siblings`` widens a CA page through the protein consequence: the sibling nt
    change encoding the same protein is pulled in as a ``nucleotide_encoding`` while the direct measurement
    is unchanged. A no-op for a PA query, which already returns every encoding."""
    score_set = setup_lib_db_with_score_set
    nt1 = _allele(session, "nt-1", level="cdna", clingen_allele_id="CA111")
    nt2 = _allele(session, "nt-2", level="cdna", clingen_allele_id="CA222")
    prot = _allele(session, "prot-P", level="protein", clingen_allele_id="PA9")

    a = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    ra = _record(session, a, assay_level="cdna")
    _link(session, ra, nt1, is_authoritative=True)
    _link(session, ra, prot)

    c = _variant(session, score_set, 2, data={"score_data": {"score": 1.0}})
    rc = _record(session, c, assay_level="cdna")
    _link(session, rc, nt2, is_authoritative=True)
    _link(session, rc, prot)

    result = get_allele_measurements(session, "CA111", user_data=_user_data(session), include_nucleotide_siblings=True)
    by_urn = {m.variant_urn: m for m in result}
    assert set(by_urn) == {a.urn, c.urn}
    assert by_urn[a.urn].relationship == "direct"
    assert by_urn[c.urn].relationship == "nucleotide_encoding"
    # Display order: the directly-measured change precedes its sibling nucleotide encoding.
    assert [m.variant_urn for m in result] == [a.urn, c.urn]

    # No-op for a protein anchor — it already returns every nt encoding.
    pa = get_allele_measurements(session, "PA9", user_data=_user_data(session), include_nucleotide_siblings=True)
    assert {m.variant_urn for m in pa} == {a.urn, c.urn}


@pytest.mark.integration
def test_private_score_set_measurement_excluded(session, setup_lib_db_with_score_set):
    """Score-set READ gates inclusion: a measurement in a score set the caller cannot read never leaks,
    even though it links the same (public, content-addressed) allele."""
    score_set = setup_lib_db_with_score_set  # private, owned by TEST_USER
    other = _second_score_set(session, score_set, "urn:mavedb:00000002-a-1", owner_username=EXTRA_USER["username"])
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")

    visible = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, visible, assay_level="cdna"), nt, is_authoritative=True)
    hidden = _variant(session, other, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, hidden, assay_level="cdna"), nt, is_authoritative=True)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))  # TEST_USER
    assert {m.variant_urn for m in result} == {visible.urn}


@pytest.mark.integration
def test_calibration_gate_withholds_classification(session, setup_lib_db_with_score_set):
    """Calibration READ gates only the inline classification: on a public score set with a private
    calibration, an anonymous caller still sees the measurement but its classification is withheld, while
    the owner sees both."""
    score_set = setup_lib_db_with_score_set
    public = _second_score_set(session, score_set, "urn:mavedb:00000002-a-1", private=False)
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, public, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)
    _calibration(session, public, variants=[variant], functional="abnormal", private=True)

    anonymous = get_allele_measurements(session, "CA123", user_data=None)
    assert len(anonymous) == 1
    assert anonymous[0].preferred_classification is None

    owner = get_allele_measurements(session, "CA123", user_data=_user_data(session))
    assert owner[0].preferred_classification is not None
    assert owner[0].preferred_classification.functional_classification.value == "abnormal"


@pytest.mark.integration
def test_preferred_classification_cascade_beats_strength(session, setup_lib_db_with_score_set):
    """With no primary calibration, the preference cascade dominates evidence strength: an
    investigator-provided calibration wins over a stronger non-RUO one and a stronger-still RUO one."""
    score_set = setup_lib_db_with_score_set
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)

    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=1000.0, research_use_only=True)
    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=100.0)  # plain, non-RUO
    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=2.0, investigator_provided=True)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))

    # Investigator-provided (weakest) is chosen over the stronger non-RUO and RUO calibrations.
    assert result[0].preferred_classification.oddspaths_ratio == 2.0


@pytest.mark.integration
def test_preferred_classification_strongest_within_tier(session, setup_lib_db_with_score_set):
    """Within one cascade tier (here two plain non-RUO calibrations), the strongest evidence wins."""
    score_set = setup_lib_db_with_score_set
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)

    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=2.0)
    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=100.0)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))

    assert result[0].preferred_classification.oddspaths_ratio == 100.0


@pytest.mark.integration
def test_research_use_only_calibrations_are_excluded(session, setup_lib_db_with_score_set):
    """A research-use-only calibration is excluded from the cascade (even if it is the strongest
    evidence)."""
    score_set = setup_lib_db_with_score_set
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)

    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=100.0, research_use_only=True)
    _calibration(session, score_set, variants=[variant], primary=False, oddspaths_ratio=1000.0, research_use_only=True)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))

    assert result[0].preferred_classification is None


@pytest.mark.integration
def test_superseded_excluded_by_default_included_opt_in(session, setup_lib_db_with_score_set):
    """Superseded measurements are current-tail-only by default; ``include_superseded`` opts in and they
    self-describe (``is_current`` false + the superseding score set's URN)."""
    score_set = setup_lib_db_with_score_set
    _second_score_set(session, score_set, "urn:mavedb:00000001-a-2", superseded_score_set_id=score_set.id)
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)

    assert get_allele_measurements(session, "CA123", user_data=_user_data(session)) == []

    opted = get_allele_measurements(session, "CA123", user_data=_user_data(session), include_superseded=True)
    assert len(opted) == 1
    assert opted[0].is_current is False
    assert opted[0].superseded_by_score_set == "urn:mavedb:00000001-a-2"


@pytest.mark.integration
def test_unreadable_superseding_reads_as_current(session, setup_lib_db_with_score_set):
    """A superseding version the caller cannot read is not leaked — the measurement reads as current and
    is included by default."""
    score_set = setup_lib_db_with_score_set  # owned by TEST_USER
    _second_score_set(
        session,
        score_set,
        "urn:mavedb:00000001-a-2",
        owner_username=EXTRA_USER["username"],
        superseded_score_set_id=score_set.id,
    )
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    variant = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, variant, assay_level="cdna"), nt, is_authoritative=True)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))  # TEST_USER
    assert len(result) == 1
    assert result[0].is_current is True
    assert result[0].superseded_by_score_set is None


@pytest.mark.integration
def test_ordering_direct_first_then_strongest_pathogenic(session, setup_lib_db_with_score_set):
    """Default order: direct before related; within a group strongest evidence first, pathogenic before
    benign at equal magnitude; the unclassified related measurement sorts last."""
    score_set = setup_lib_db_with_score_set
    nt = _allele(session, "nt-N", level="cdna", clingen_allele_id="CA123")
    prot = _allele(session, "prot-P", level="protein", clingen_allele_id="PA9")

    strong_path = _variant(session, score_set, 1, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, strong_path, assay_level="cdna"), nt, is_authoritative=True)
    _calibration(session, score_set, variants=[strong_path], functional="abnormal", oddspaths_ratio=100.0)

    strong_benign = _variant(session, score_set, 2, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, strong_benign, assay_level="cdna"), nt, is_authoritative=True)
    _calibration(session, score_set, variants=[strong_benign], functional="normal", oddspaths_ratio=0.01)

    weak_path = _variant(session, score_set, 3, data={"score_data": {"score": 1.0}})
    _link(session, _record(session, weak_path, assay_level="cdna"), nt, is_authoritative=True)
    _calibration(session, score_set, variants=[weak_path], functional="abnormal", oddspaths_ratio=2.0)

    related = _variant(session, score_set, 4, data={"score_data": {"score": 1.0}})
    related_record = _record(session, related, assay_level="protein")
    _link(session, related_record, prot, is_authoritative=True)
    _link(session, related_record, nt)

    result = get_allele_measurements(session, "CA123", user_data=_user_data(session))

    assert [m.variant_urn for m in result] == [strong_path.urn, strong_benign.urn, weak_path.urn, related.urn]


@pytest.mark.integration
def test_unknown_clingen_id_returns_empty(session, setup_lib_db_with_score_set):
    assert get_allele_measurements(session, "CA000", user_data=_user_data(session)) == []


def _oddspaths_classification(ratio):
    """A transient functional classification carrying only an oddspaths ratio — enough for
    ``classification_evidence_strength`` (which reads ``acmg_classification`` [None here] then the ratio)
    to rank it, with no DB round-trip."""
    return ScoreCalibrationFunctionalClassification(oddspaths_ratio=ratio)


def _measurement(urn, *, relationship=MeasurementRelationship.direct, classification=None, is_current=True):
    """A transit ``AlleleMeasurement`` with just the fields ``_ordering_key`` reads; the rest are filler."""
    return AlleleMeasurement(
        variant_urn=urn,
        score=None,
        assay_level=None,
        relationship=relationship,
        assay_level_hgvs=None,
        submitted_hgvs=None,
        score_set_urn="",
        score_set_title="",
        preferred_classification=classification,
        is_current=is_current,
        superseded_by_score_set=None,
    )


def test_ordering_key_ranks_every_dimension():
    """Pin the full precedence chain of ``_ordering_key`` as a pure unit (no DB, no calibration fixtures):
    current → direct → classified → strongest magnitude → pathogenic direction → newest published → urn.
    Each adjacent pair below differs by exactly the next-lower key, so sorting a shuffled copy must
    reproduce this exact order."""
    strong_path = _oddspaths_classification(100.0)  # magnitude |ln 100|, pathogenic
    strong_benign = _oddspaths_classification(0.01)  # equal magnitude, benign
    weak_path = _oddspaths_classification(2.0)  # smaller magnitude, pathogenic

    # (measurement, published_date), already in the expected ranked order.
    ranked = [
        (_measurement("u#1", classification=strong_path), date(2024, 1, 1)),  # strongest evidence, pathogenic
        (_measurement("u#2", classification=strong_benign), date(2024, 1, 1)),  # equal magnitude, benign loses
        (_measurement("u#3", classification=weak_path), date(2024, 1, 1)),  # weaker magnitude
        (_measurement("u#4a", classification=None), date(2024, 6, 1)),  # unclassified; newer published leads its tie
        (_measurement("u#4b", classification=None), date(2024, 1, 1)),  # unclassified, older published
        (_measurement("u#4c", classification=None), date(2024, 1, 1)),  # unclassified, same date → urn tiebreak
        (_measurement("u#5", relationship=MeasurementRelationship.protein_consequence), date(2024, 6, 1)),  # related
        (_measurement("u#6", is_current=False), date(2024, 6, 1)),  # superseded sorts last
    ]

    shuffled = [ranked[i] for i in (5, 0, 7, 2, 4, 1, 6, 3)]
    ordered = sorted(shuffled, key=lambda pair: _ordering_key(pair[0], pair[1]))

    assert [m.variant_urn for m, _ in ordered] == [m.variant_urn for m, _ in ranked]


def test_ordering_key_missing_published_date_sorts_after_dated():
    """A null published date is treated as oldest (``0`` ordinal), so a dated measurement leads an
    otherwise-identical undated one."""
    dated = (_measurement("u#dated"), date(2024, 1, 1))
    undated = (_measurement("u#undated"), None)

    ordered = sorted([undated, dated], key=lambda pair: _ordering_key(pair[0], pair[1]))

    assert [m.variant_urn for m, _ in ordered] == ["u#dated", "u#undated"]
