from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.util import (
    change_ownership,
    create_experiment,
    create_seq_score_set_with_variants,
)


def test_search_my_target_genes_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )

    search_payload = {"text": "NONEXISTENT"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_my_target_genes_no_match_on_other_user(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_my_target_genes_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )

    search_payload = {"text": "TEST1"}
    response = client.post("/api/v1/target-genes/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "TEST1"
