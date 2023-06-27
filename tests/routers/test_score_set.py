from pathlib import Path
import jsonschema
import re
from copy import deepcopy
import pytest
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.constants import TEST_MINIMAL_SCORE_SET, TEST_MINIMAL_SCORE_SET_RESPONSE
from tests.helpers.util import create_experiment, create_score_set, change_ownership, create_score_set_with_variants
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE


def test_test_minimal_score_set_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_SCORE_SET, schema=ScoreSetCreate.schema())


def test_create_minimal_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experiment"]["urn"] = experiment["urn"]
    expected_response["experiment"]["experimentSetUrn"] = experiment["experimentSetUrn"]
    expected_response["experiment"]["scoreSetUrns"] = [response_data["urn"]]
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])
    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_get_own_private_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response.update({"urn": score_set["urn"]})
    expected_response["experiment"].update(
        {
            "urn": experiment["urn"],
            "experimentSetUrn": experiment["experimentSetUrn"],
            "scoreSetUrns": [score_set["urn"]],
        }
    )
    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_cannot_get_other_user_private_score_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_add_score_set_variants_scores_only(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert response_data["numVariants"] == 3
    assert response_data["datasetColumns"] == {"countColumns": [], "scoreColumns": ["score"]}


def test_add_score_set_variants_scores_and_counts(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    with open(scores_csv_path, "rb") as scores_file, open(counts_csv_path, "rb") as counts_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert response_data["numVariants"] == 3
    assert response_data["datasetColumns"] == {"countColumns": ["c_0", "c_1"], "scoreColumns": ["score"]}


def test_cannot_add_scores_to_other_user_score_set(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    scores_csv_path = data_files / "scores.csv"
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_publish_score_set(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["urn"] == "urn:mavedb:00000001-a-1"
    assert response_data["experiment"]["urn"] == "urn:mavedb:00000001-a"


def test_cannot_publish_score_set_without_variants(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_score_set(client, experiment["urn"])
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 422
    response_data = response.json()
    assert f"cannot publish score set without variant scores" in response_data["detail"]


def test_cannot_publish_other_user_private_score_set(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_create_single_published_score_set_meta_analysis(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    meta_score_set = create_score_set(
        client, None, update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]}
    )
    score_set_refresh = client.get(f"/api/v1/score-sets/{score_set['urn']}").json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == [score_set["urn"]]
    assert score_set_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_multiple_published_score_set_meta_analysis_single_experiment(client, setup_router_db):
    pass


def test_multiple_published_score_set_meta_analysis_multiple_experiment(client, setup_router_db):
    pass
