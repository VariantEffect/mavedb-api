# ruff: noqa: E402
"""Router tests for the ClinGen-allele measurements endpoint
(``GET /clingen-alleles/{caid}/measurements``).

Exercise the HTTP surface: the equivalence-class list serializes and validates, the ``as_of``
content-time is echoed in ``X-As-Of``, an unknown id is an empty list (not a 404), superseded
measurements are opt-in, and a private score set's measurement never leaks.
"""

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from sqlalchemy import select

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.view_models.allele_measurement import AlleleMeasurement
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set_with_variants
from tests.helpers.util.user import change_ownership


def _seed_cdna_measurement(session, variant_urn, *, clingen_allele_id="CA123"):
    """Give a variant a live coding-measured mapping record whose authoritative allele carries the
    ClinGen id — the minimal shape for a direct measurement on that CA's page."""
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == variant_urn))
    record = MappingRecord(
        variant_id=variant.id,
        assay_level="cdna",
        hgvs_assay_level="NM_000546.6:c.1216G>A",
        mapping_api_version="test.0.0",
    )
    session.add(record)
    session.commit()

    measured = Allele(vrs_digest="cdna-digest", level="cdna", clingen_allele_id=clingen_allele_id)
    session.add(measured)
    session.commit()
    session.add(MappingRecordAllele(mapping_record_id=record.id, allele_id=measured.id, is_authoritative=True))
    session.commit()


def test_measurements_serialize(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    urn = f"{score_set['urn']}#1"
    _seed_cdna_measurement(session, urn)

    response = client.get("/api/v1/clingen-alleles/CA123/measurements")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    AlleleMeasurement.model_validate(body[0])
    assert body[0]["variantUrn"] == urn
    assert body[0]["relationship"] == "direct"
    assert body[0]["assayLevel"] == "cdna"
    assert body[0]["assayLevelHgvs"] == "NM_000546.6:c.1216G>A"
    assert body[0]["submittedHgvs"] == "c.1A>T"
    assert body[0]["scoreSetUrn"] == score_set["urn"]
    assert body[0]["isCurrent"] is True
    assert "supersededByScoreSet" not in body[0]  # dropped by exclude_none when current
    assert response.headers["X-As-Of"] == "current"


def test_unknown_clingen_id_returns_empty_list(client, setup_router_db):
    response = client.get("/api/v1/clingen-alleles/CA000/measurements")
    assert response.status_code == 200
    assert response.json() == []


def test_superseded_measurement_is_opt_in(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    older = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    newer = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    older_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == older["urn"]))
    newer_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == newer["urn"]))
    newer_db.superseded_score_set_id = older_db.id
    session.commit()
    _seed_cdna_measurement(session, f"{older['urn']}#1")

    default = client.get("/api/v1/clingen-alleles/CA123/measurements")
    assert default.status_code == 200
    assert default.json() == []

    opted = client.get("/api/v1/clingen-alleles/CA123/measurements", params={"include_superseded": True})
    assert opted.status_code == 200
    body = opted.json()
    assert len(body) == 1
    assert body[0]["isCurrent"] is False
    assert body[0]["supersededByScoreSet"] == newer["urn"]


def test_nucleotide_siblings_are_opt_in(client, session, data_provider, data_files, setup_router_db):
    """The sibling nt bucket is a discovery opt-in: a different DNA variant encoding the same protein
    consequence is absent by default and pulled in as a ``nucleotide_encoding`` under
    ``include_nucleotide_siblings``."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    prot = Allele(vrs_digest="prot-digest", level="protein", clingen_allele_id="PA9")
    session.add(prot)
    session.commit()

    # Two coding measurements encoding the same protein consequence (PA9), each carrying its own CA.
    for suffix, caid, digest in ((1, "CA111", "cdna-1"), (2, "CA222", "cdna-2")):
        variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == f"{score_set['urn']}#{suffix}"))
        record = MappingRecord(variant_id=variant.id, assay_level="cdna", mapping_api_version="test.0.0")
        session.add(record)
        session.commit()
        nt = Allele(vrs_digest=digest, level="cdna", clingen_allele_id=caid)
        session.add(nt)
        session.commit()
        session.add(MappingRecordAllele(mapping_record_id=record.id, allele_id=nt.id, is_authoritative=True))
        session.add(MappingRecordAllele(mapping_record_id=record.id, allele_id=prot.id))
        session.commit()

    default = client.get("/api/v1/clingen-alleles/CA111/measurements")
    assert {m["variantUrn"] for m in default.json()} == {f"{score_set['urn']}#1"}

    widened = client.get("/api/v1/clingen-alleles/CA111/measurements", params={"include_nucleotide_siblings": True})
    assert widened.status_code == 200
    by_urn = {m["variantUrn"]: m for m in widened.json()}
    assert set(by_urn) == {f"{score_set['urn']}#1", f"{score_set['urn']}#2"}
    assert by_urn[f"{score_set['urn']}#1"]["relationship"] == "direct"
    assert by_urn[f"{score_set['urn']}#2"]["relationship"] == "nucleotide_encoding"


def test_anonymous_cannot_see_private_measurement(
    client, session, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed_cdna_measurement(session, f"{score_set['urn']}#1")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/clingen-alleles/CA123/measurements")

    # A content-addressed ClinGen id is public, but a private score set's measurement is filtered out.
    assert response.status_code == 200
    assert response.json() == []
