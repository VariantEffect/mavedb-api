# ruff: noqa: E402
import pytest
from copy import deepcopy
from unittest.mock import patch

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel

from tests.helpers.constants import TEST_MINIMAL_SEQ_SCORESET, TEST_USER
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.contributor import add_contributor
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
    assert response.json()[0]["scoreSetUrn"] == score_set["urn"]


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
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"
    assert response.json()[0]["scoreSetUrn"] == published_score_set["urn"]


def test_search_target_genes_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"
    assert response.json()[0]["scoreSetUrn"] == score_set["urn"]


def test_fetch_target_gene_by_valid_id(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    response = client.get("/api/v1/target-genes/1")
    assert response.status_code == 200
    assert response.json()["scoreSetUrn"] == score_set["urn"]


def test_fetch_target_gene_by_invalid_id(client, setup_router_db):
    response = client.get("/api/v1/target-genes/1")
    assert response.status_code == 404


def test_fetch_private_target_gene_by_id_without_permission(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get("/api/v1/target-genes/1")
    assert response.status_code == 404


def test_fetch_private_target_gene_by_id_with_permission(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )

    response = client.get("/api/v1/target-genes/1")
    assert response.status_code == 200
    assert response.json()["scoreSetUrn"] == score_set["urn"]


def test_fetch_public_target_gene_by_id(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)

    response = client.get("/api/v1/target-genes/1")
    assert response.status_code == 200
    assert response.json()["scoreSetUrn"] == published_score_set["urn"]


def _score_set_with_target(client, experiment_urn, name, category):
    """Create a score set whose single target gene carries the given name and category."""
    target_genes = deepcopy(TEST_MINIMAL_SEQ_SCORESET["targetGenes"])
    target_genes[0]["name"] = name
    target_genes[0]["category"] = category
    return create_seq_score_set(client, experiment_urn, update={"targetGenes": target_genes})


def _published_and_unpublished_targets(session, data_provider, client, data_files):
    """Put one published and one unpublished score set, with distinct target genes, in one experiment.

    The unpublished score set is created after the publish, against the experiment's published URN, so that
    it sits inside a public experiment. That is the arrangement in which its target gene leaks.
    """
    experiment = create_experiment(client, {"title": "Experiment 1"})
    published = _score_set_with_target(client, experiment["urn"], "PUBLISHEDGENE", "protein_coding")
    published = mock_worker_variant_insertion(client, session, data_provider, published, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, published["urn"])

    _score_set_with_target(client, published["experiment"]["urn"], "UNPUBLISHEDGENE", "regulatory")


def test_list_target_gene_names_excludes_unpublished(session, data_provider, client, setup_router_db, data_files):
    """Target gene names from unpublished score sets are not disclosed by this unauthenticated route.

    Asserted against one published and one unpublished score set so that the test fails whether the filter
    is missing or too aggressive.
    """
    _published_and_unpublished_targets(session, data_provider, client, data_files)

    response = client.get("/api/v1/target-genes/names")
    assert response.status_code == 200
    assert "PUBLISHEDGENE" in response.json()
    assert "UNPUBLISHEDGENE" not in response.json()


def test_list_target_gene_categories_excludes_unpublished(session, data_provider, client, setup_router_db, data_files):
    _published_and_unpublished_targets(session, data_provider, client, data_files)

    response = client.get("/api/v1/target-genes/categories")
    assert response.status_code == 200
    assert "protein_coding" in response.json()
    assert "regulatory" not in response.json()


def test_anonymous_list_target_gene_names_excludes_unpublished(
    session, data_provider, client, anonymous_app_overrides, setup_router_db, data_files
):
    _published_and_unpublished_targets(session, data_provider, client, data_files)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/target-genes/names")

    assert response.status_code == 200
    assert "PUBLISHEDGENE" in response.json()
    assert "UNPUBLISHEDGENE" not in response.json()
