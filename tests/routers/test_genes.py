# ruff: noqa: E402

from copy import deepcopy
from datetime import date
from unittest.mock import patch
from urllib.parse import quote

import pytest

pytestmark = pytest.mark.unit

fastapi = pytest.importorskip("fastapi")

from mavedb.lib.exceptions import HGNCGeneNotFoundError, HGNCServiceError
from mavedb.lib.hgnc.client import HGNCGeneInfo
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.constants import TEST_MINIMAL_SEQ_SCORESET, VALID_GENE
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_multi_target_score_set, create_seq_score_set

INVALID_GENE = "NOTAREAL"
SLASH_GENE = "TRAV29/DV5"


def _hgnc_gene_info(symbol=VALID_GENE):
    return HGNCGeneInfo(
        symbol=symbol,
        name="BRCA1 DNA repair associated",
        hgnc_id="HGNC:1100",
        locus_group="protein-coding gene",
        location="17q21.31",
        omim_id="113705",
    )


def _make_score_set(
    client,
    session,
    *,
    symbol=VALID_GENE,
    title="Gene endpoint score set",
    private=False,
    published=True,
    num_variants=7,
):
    experiment = create_experiment(client, {"title": f"{title} experiment"})
    payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    payload["title"] = title
    payload["shortDescription"] = f"{title} short description"
    score_set = create_seq_score_set(client, experiment["urn"], payload)

    db_score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set["urn"]).one()
    db_score_set.private = private
    db_score_set.published_date = date(2024, 1, 1) if published else None
    db_score_set.num_variants = num_variants
    for target_gene in db_score_set.target_genes:
        target_gene.mapped_hgnc_name = symbol
    session.commit()
    return db_score_set.urn


def _make_multi_target_score_set(client, session, second_symbol="TP53"):
    experiment = create_experiment(client, {"title": "Multi-target experiment"})
    score_set = create_multi_target_score_set(client, experiment["urn"])
    db_score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set["urn"]).one()
    db_score_set.private = False
    db_score_set.published_date = date(2024, 1, 1)
    db_score_set.num_variants = 11
    db_score_set.target_genes[0].mapped_hgnc_name = VALID_GENE
    db_score_set.target_genes[1].mapped_hgnc_name = second_symbol
    session.commit()
    return db_score_set.urn


def test_get_gene_valid_with_data(client, session, setup_router_db):
    visible_score_set_urn = _make_score_set(client, session, title="BRCA1 visible score set", num_variants=13)

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    mock_fetch.assert_called_once_with(VALID_GENE)
    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == VALID_GENE
    assert body["name"] == "BRCA1 DNA repair associated"
    assert body["hgncId"] == "HGNC:1100"
    assert body["locusGroup"] == "protein-coding gene"
    assert body["location"] == "17q21.31"
    assert body["omimId"] == "113705"
    assert body["limit"] == 20
    assert body["offset"] == 0
    assert body["total"] == 1
    assert body["totalScoredVariants"] == 13
    assert body["scoreSets"][0]["urn"] == visible_score_set_urn
    assert body["scoreSets"][0]["numVariants"] == 13
    assert body["scoreSets"][0]["targetGenes"][0]["mappedHgncName"] == VALID_GENE


def test_get_gene_valid_no_data(client, setup_router_db):
    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 200
    body = response.json()
    assert body["scoreSets"] == []
    assert body["total"] == 0
    assert body["totalScoredVariants"] == 0


def test_get_gene_slash_symbol_returns_404():
    # No approved HGNC symbols currently contain a slash — the GeneSymbolConverter regex
    # intentionally excludes them. This test documents that behaviour: a percent-encoded
    # slash in the URL path is not routed to the gene handler.
    from mavedb.server_main import app
    from starlette.testclient import TestClient as StarletteTestClient

    with StarletteTestClient(app) as c:
        response = c.get(f"/api/v1/genes/{quote(SLASH_GENE, safe='')}")

    assert response.status_code == 404


def test_get_gene_multi_target_includes_all_target_genes(client, session, setup_router_db):
    multi_target_score_set_urn = _make_multi_target_score_set(client, session)

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 200
    score_set = response.json()["scoreSets"][0]
    assert score_set["urn"] == multi_target_score_set_urn
    assert len(score_set["targetGenes"]) == 2
    assert {target["mappedHgncName"] for target in score_set["targetGenes"]} == {VALID_GENE, "TP53"}


def test_get_gene_multi_target_same_gene_counted_once(client, session, setup_router_db):
    multi_target_score_set_urn = _make_multi_target_score_set(client, session, second_symbol=VALID_GENE)

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}?limit=1")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["totalScoredVariants"] == 11
    assert len(body["scoreSets"]) == 1
    assert body["scoreSets"][0]["urn"] == multi_target_score_set_urn


def test_get_gene_invalid_symbol(client, setup_router_db):
    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.side_effect = HGNCGeneNotFoundError(f"Gene symbol not found: {INVALID_GENE}")
        response = client.get(f"/api/v1/genes/{INVALID_GENE}")

    assert response.status_code == 404


def test_get_gene_private_excluded(client, session, setup_router_db):
    private_score_set_urn = _make_score_set(client, session, title="BRCA1 private score set", private=True)

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 200
    returned_urns = {score_set["urn"] for score_set in response.json()["scoreSets"]}
    assert private_score_set_urn not in returned_urns
    assert response.json()["total"] == 0
    assert response.json()["totalScoredVariants"] == 0


def test_get_gene_unpublished_excluded(client, session, setup_router_db):
    unpublished_score_set_urn = _make_score_set(
        client, session, title="BRCA1 unpublished score set", private=False, published=False
    )

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 200
    returned_urns = {score_set["urn"] for score_set in response.json()["scoreSets"]}
    assert unpublished_score_set_urn not in returned_urns
    assert response.json()["total"] == 0
    assert response.json()["totalScoredVariants"] == 0


def test_get_gene_superseded_excluded(client, session, setup_router_db):
    superseded_urn = _make_score_set(client, session, title="BRCA1 superseded score set", num_variants=3)
    superseding_urn = _make_score_set(client, session, title="BRCA1 superseding score set", num_variants=5)

    superseded = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == superseded_urn).one()
    superseding = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == superseding_urn).one()
    superseding.superseded_score_set_id = superseded.id
    session.commit()

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 200
    body = response.json()
    returned_urns = {ss["urn"] for ss in body["scoreSets"]}
    assert superseded_urn not in returned_urns
    assert superseding_urn in returned_urns
    assert body["total"] == 1


def test_get_gene_pagination(client, session, setup_router_db):
    first_urn = _make_score_set(client, session, title="BRCA1 first score set", num_variants=5)
    second_urn = _make_score_set(client, session, title="BRCA1 second score set", num_variants=7)
    third_urn = _make_score_set(client, session, title="BRCA1 third score set", num_variants=11)

    first = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == first_urn).one()
    second = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == second_urn).one()
    third = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == third_urn).one()
    first.published_date = date(2024, 1, 3)
    second.published_date = date(2024, 1, 2)
    third.published_date = date(2024, 1, 1)
    session.commit()

    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.return_value = _hgnc_gene_info()
        response = client.get(f"/api/v1/genes/{VALID_GENE}?limit=1&offset=1")

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert body["total"] == 3
    assert body["totalScoredVariants"] == 23
    assert len(body["scoreSets"]) == 1
    assert body["scoreSets"][0]["urn"] == second_urn


def test_get_gene_hgnc_service_error_returns_503(client, setup_router_db):
    with patch("mavedb.routers.genes.fetch_gene_info") as mock_fetch:
        mock_fetch.side_effect = HGNCServiceError("Gene information service temporarily unavailable")
        response = client.get(f"/api/v1/genes/{VALID_GENE}")

    assert response.status_code == 503
    assert "Gene information service temporarily unavailable" in response.json()["message"]
