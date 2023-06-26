from pathlib import Path
import jsonschema
import re
from copy import deepcopy

from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.conftest import (
    client,
    change_ownership,
    TEST_MINIMAL_SCORE_SET,
    TEST_MINIMAL_SCORE_SET_RESPONSE,
)
from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE


def test_test_minimal_score_set_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_SCORE_SET, schema=ScoreSetCreate.schema())


def test_create_minimal_score_set(test_router_db):
    experiment = test_router_db
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
    expected_response["experiment"]["numScoreSets"] = 1
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]
    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_get_own_private_score_set(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response["urn"] = response_data["urn"]
    expected_response["experiment"]["urn"] = experiment["urn"]
    expected_response["experiment"]["experimentSetUrn"] = experiment["experimentSetUrn"]
    expected_response["experiment"]["numScoreSets"] = 1
    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert expected_response[key] == response_data[key]


def test_cannot_get_other_user_private_score_set(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    change_ownership(score_set_urn, ScoreSetDbModel)
    response = client.get(f"/api/v1/score-sets/{score_set_urn}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set_urn}' not found" in response_data["detail"]


def test_add_score_set_variants_scores_only(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    current_directory = Path(__file__).absolute().parent
    scores_csv_path = Path(current_directory, "test_data", "scores.csv")
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set_urn}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert response_data["numVariants"] == 3
    assert response_data["datasetColumns"] == {"countColumns": [], "scoreColumns": ["score"]}


def test_add_score_set_variants_scores_and_counts(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    current_directory = Path(__file__).absolute().parent
    scores_csv_path = Path(current_directory, "test_data", "scores.csv")
    counts_csv_path = Path(current_directory, "test_data", "counts.csv")
    with open(scores_csv_path, "rb") as scores_file, open(counts_csv_path, "rb") as counts_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set_urn}/variants/data",
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


def test_cannot_add_scores_to_other_user_score_set(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    change_ownership(score_set_urn, ScoreSetDbModel)
    current_directory = Path(__file__).absolute().parent
    scores_csv_path = Path(current_directory, "test_data", "scores.csv")
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set_urn}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set_urn}' not found" in response_data["detail"]


def test_cannot_publish_score_set_without_variants(test_router_db):
    experiment = test_router_db
    score_set_post_payload = deepcopy(TEST_MINIMAL_SCORE_SET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    response_data = response.json()
    score_set_urn = response_data["urn"]
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish", json=score_set_post_payload)
    assert response.status_code == 422
    assert f"cannot publish score set without variant scores" in response_data["detail"]


def test_single_published_score_set_meta_analysis(test_with_empty_db):
    pass


def test_multiple_published_score_set_meta_analysis_single_experiment(test_with_empty_db):
    pass


def test_multiple_published_score_set_meta_analysis_multiple_experiment(test_with_empty_db):
    pass
