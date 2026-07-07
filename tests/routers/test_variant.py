# ruff: noqa: E402
"""Router tests for the assayed variant-detail endpoint (``GET /variants/{urn}``).

Exercise the HTTP surface end to end: the two-tier envelope serializes and validates, the ``as_of``
content-time is echoed in the ``X-As-Of`` header, a superseded variant self-describes rather than
reading as current, and the READ gate holds for private score sets.
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
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.view_models.variant_detail import VariantDetail
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set_with_variants
from tests.helpers.util.user import change_ownership

_VALID_DIGEST = "0123456789abcdefghijABCDEFGHIJ_-"


def _post_mapped() -> dict:
    """A spec-valid post_mapped VRS Allele — the Cat-VRS builder hydrates this on the detail path."""
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


def _seed_mapping(session, variant_urn):
    """Give a variant a live coding-measured mapping record whose authoritative allele carries a
    digest + ClinGen id + a live VEP consequence, plus a protein member — enough to build Cat-VRS."""
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == variant_urn))
    record = MappingRecord(
        variant_id=variant.id,
        assay_level="cdna",
        hgvs_assay_level="NM_000546.6:c.1216G>A",
        mapping_api_version="test.0.0",
    )
    session.add(record)
    session.commit()

    measured = Allele(vrs_digest="cdna-digest", level="cdna", post_mapped=_post_mapped(), clingen_allele_id="CA123")
    protein = Allele(vrs_digest="prot-digest", level="protein", post_mapped=_post_mapped())
    session.add_all([measured, protein])
    session.commit()

    session.add_all(
        [
            MappingRecordAllele(mapping_record_id=record.id, allele_id=measured.id, is_authoritative=True),
            MappingRecordAllele(mapping_record_id=record.id, allele_id=protein.id, is_authoritative=False),
            VepAlleleConsequence(
                allele_id=measured.id,
                functional_consequence="missense_variant",
                source_version="116",
                access_date="2026-01-01",
            ),
        ]
    )
    session.commit()


def test_get_variant_detail_envelope(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    urn = f"{score_set['urn']}#1"
    _seed_mapping(session, urn)

    response = client.get(f"/api/v1/variants/{urn.replace(chr(35), '%23')}")

    assert response.status_code == 200
    body = response.json()
    VariantDetail.model_validate(body)

    assert body["urn"] == urn
    assert body["assayLevel"] == "cdna"
    assert body["referenceHgvs"] == "NM_000546.6:c.1216G>A"
    assert body["assayLevelDigest"] == "cdna-digest"
    assert body["clingenAlleleId"] == "CA123"
    assert body["mode"] == "projection"
    # Spec-pure Cat-VRS carries its own field names (no camelization of the nested GA4GH object).
    assert body["molecularRepresentation"]["type"] == "CategoricalVariant"
    assert body["memberRelations"] == {"prot-digest": "translation_of"}
    assert body["annotations"]["cdna-digest"]["vep"]["consequence"] == "missense_variant"
    assert body["isCurrent"] is True
    assert "supersededByScoreSet" not in body  # dropped by exclude_none when current
    assert response.headers["X-As-Of"] == "current"


def test_get_variant_detail_unknown_urn_is_404(client, setup_router_db):
    response = client.get("/api/v1/variants/urn:mavedb:00000000-a-1#1")
    assert response.status_code == 404


def test_get_variant_detail_echoes_as_of_header(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    urn = f"{score_set['urn']}#1"

    historical = client.get(f"/api/v1/variants/{urn.replace(chr(35), '%23')}", params={"as_of": "2020-01-01T00:00:00Z"})

    assert historical.status_code == 200
    assert historical.headers["X-As-Of"] == "2020-01-01T00:00:00+00:00"
    # Before any mapping existed, the molecular layer is empty.
    assert "molecularRepresentation" not in historical.json()


def test_superseded_variant_self_describes(client, session, data_provider, data_files, setup_router_db):
    """A variant on a superseded score set is still served, but flags its version standing."""
    experiment = create_experiment(client)
    older = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    newer = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    # Link newer as the version that replaces older (replaces_id / superseding relationship).
    older_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == older["urn"]))
    newer_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == newer["urn"]))
    newer_db.superseded_score_set_id = older_db.id
    session.commit()

    response = client.get(f"/api/v1/variants/{older['urn']}%231")

    assert response.status_code == 200
    body = response.json()
    assert body["isCurrent"] is False
    assert body["supersededByScoreSet"] == newer["urn"]


def test_get_variant_detail_anonymous_cannot_read_private(
    client, session, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/variants/{score_set['urn']}%231")

    assert response.status_code == 404
