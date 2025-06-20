# ruff: noqa: E402
import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel

from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.user import change_ownership
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set
from tests.helpers.util.variant import mock_worker_variant_insertion


def test_search_my_target_genes_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "NONEXISTENT"}
    response = client.post("/api/v1/me/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_my_target_genes_no_match_on_other_user(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/me/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_my_target_genes_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/me/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"


def test_search_target_genes_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "NONEXISTENT"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_private_target_genes_match_on_other_user(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_public_target_genes_match_on_other_user(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    published_score_set = publish_score_set(client, score_set["urn"])
    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"


def test_search_target_genes_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"
