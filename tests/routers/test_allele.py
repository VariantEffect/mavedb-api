# ruff: noqa: E402
"""Router tests for the allele-detail endpoints (``GET /alleles/{digest}`` and ``?clingenAlleleId=``).

Exercise the HTTP surface end to end: the envelope serializes and validates, the equivalence class is
labelled relative to the focus, CAID fetch focuses the nt-canonical change, ``as_of`` is echoed and
reconstructs membership, an unknown id is a plain 404 — and, unlike ``GET /variants/{urn}``, the
endpoint is **public with no privacy gate**: an allele reachable only through a private score set is
still served.
"""

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.view_models.allele_detail import AlleleDetail
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set_with_variants
from tests.helpers.util.user import change_ownership

_VALID_DIGEST = "0123456789abcdefghijABCDEFGHIJ_-"


def _ir(tag: str) -> str:
    """A valid GA4GH IR (``ga4gh:VA.`` + exactly 32 base64url chars), tagged for readability."""
    return f"ga4gh:VA.{tag.ljust(32, '0')}"


CDNA = _ir("cdna")
GEN = _ir("gen")
PROT = _ir("prot")
COUSIN = _ir("cousin")


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


def _seed(session, variant_urn):
    """Coding-measured record: authoritative cdna (+ClinGen CA1 +VEP), its genomic partner (also CA1),
    the protein apex, and a synonymous cousin in its own projection group."""
    seed_mapping_record(
        session,
        variant_urn,
        alleles=[
            AlleleSpec(
                digest=CDNA,
                level="cdna",
                is_authoritative=True,
                clingen_allele_id="CA1",
                hgvs_c="NM_000546.6:c.1216G>A",
                post_mapped=_post_mapped(),
                vep_consequence="missense_variant",
                projection_group=0,
            ),
            AlleleSpec(
                digest=GEN,
                level="genomic",
                clingen_allele_id="CA1",
                hgvs_g="NC_000017.11:g.7676154C>T",
                post_mapped=_post_mapped(),
                projection_group=0,
            ),
            AlleleSpec(
                digest=PROT,
                level="protein",
                clingen_allele_id="PA1",
                hgvs_p="NP_000537.3:p.Ala406Thr",
                post_mapped=_post_mapped(),
            ),
            AlleleSpec(
                digest=COUSIN,
                level="cdna",
                hgvs_c="NM_000546.6:c.1218C>T",
                post_mapped=_post_mapped(),
                projection_group=1,
            ),
        ],
        assay_level="cdna",
    )


def test_get_allele_detail_envelope(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed(session, f"{score_set['urn']}#1")

    response = client.get(f"/api/v1/alleles/{CDNA}")

    assert response.status_code == 200
    body = response.json()
    AlleleDetail.model_validate(body)

    assert body["digest"] == CDNA
    assert body["level"] == "cdna"
    assert body["hgvs"] == "NM_000546.6:c.1216G>A"
    assert body["clingenAlleleId"] == "CA1"
    assert body["vrs"]["type"] == "Allele"
    # Full equivalence class, keyed by digest, labelled relative to the queried allele (camelCase).
    assert set(body["alleles"]) == {CDNA, GEN, PROT, COUSIN}
    assert body["alleles"][CDNA]["isFocus"] is True
    assert "derivation" not in body["alleles"][CDNA]  # focus carries none; dropped by exclude_none
    assert body["alleles"][GEN]["relation"] == "coordinate_representation_of"
    assert body["alleles"][GEN]["derivation"] == "projection"
    assert body["alleles"][PROT]["relation"] == "translation_of"
    assert body["alleles"][COUSIN]["relation"] == "co_encodes"
    assert body["alleles"][COUSIN]["derivation"] == "convergent"
    # Annotations share keys with the class and join by digest.
    assert set(body["annotations"]) == {CDNA, GEN, PROT, COUSIN}
    assert body["annotations"][CDNA]["vep"]["consequence"] == "missense_variant"
    # Measurement-agnostic: no variant-detail measurement fields.
    assert "molecularRepresentation" not in body
    assert "classifications" not in body
    assert "isCurrent" not in body
    assert response.headers["X-As-Of"] == "current"


def test_get_allele_detail_by_caid(client, session, data_provider, data_files, setup_router_db):
    """CAID fetch on the overloaded `/alleles/{identifier}` path focuses the nt-canonical change (both
    genomic + coding frames isFocus)."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed(session, f"{score_set['urn']}#1")

    response = client.get("/api/v1/alleles/CA1")

    assert response.status_code == 200
    body = response.json()
    AlleleDetail.model_validate(body)
    assert body["clingenAlleleId"] == "CA1"
    assert body["alleles"][CDNA]["isFocus"] is True
    assert body["alleles"][GEN]["isFocus"] is True  # the CAID's other frame
    assert body["alleles"][COUSIN]["derivation"] == "convergent"


def test_get_allele_detail_by_paid(client, session, data_provider, data_files, setup_router_db):
    """PAID fetch focuses the protein allele; its nucleotide equivalents surface as candidates — the
    same `/alleles/{identifier}` path, no separate route or param."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed(session, f"{score_set['urn']}#1")

    response = client.get("/api/v1/alleles/PA1")

    assert response.status_code == 200
    body = response.json()
    AlleleDetail.model_validate(body)
    assert body["clingenAlleleId"] == "PA1"
    assert body["alleles"][PROT]["isFocus"] is True
    # Walking down from the protein is ambiguous: every nucleotide member is a candidate.
    for nt in (CDNA, GEN, COUSIN):
        assert body["alleles"][nt]["derivation"] == "candidate", nt


def test_get_allele_detail_unknown_digest_is_404(client, setup_router_db):
    response = client.get(f"/api/v1/alleles/{_ir('nonexistent')}")
    assert response.status_code == 404


def test_get_allele_detail_unknown_caid_is_404(client, setup_router_db):
    response = client.get("/api/v1/alleles/CA999999")
    assert response.status_code == 404


def test_get_allele_detail_is_public_no_privacy_gate(
    client, session, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    """The decision: no privacy gate. An allele reachable only through a private score set is still
    served to an anonymous caller — contrast ``GET /variants/{urn}``, which 404s the same variant."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed(session, f"{score_set['urn']}#1")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/alleles/{CDNA}")

    assert response.status_code == 200
    assert response.json()["digest"] == CDNA


def test_get_allele_detail_echoes_as_of_header(client, session, data_provider, data_files, setup_router_db):
    """``as_of`` is echoed and reconstructs membership: before the record was live the class collapses
    to the focus (the allele row itself is content-addressed, so it still resolves)."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    _seed(session, f"{score_set['urn']}#1")

    response = client.get(f"/api/v1/alleles/{CDNA}", params={"as_of": "2020-01-01T00:00:00Z"})

    assert response.status_code == 200
    assert response.headers["X-As-Of"] == "2020-01-01T00:00:00+00:00"
    assert set(response.json()["alleles"]) == {CDNA}
