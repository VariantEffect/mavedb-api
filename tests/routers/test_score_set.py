from pathlib import Path
import jsonschema
import re
from copy import deepcopy
from datetime import date
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
    expected_response.update({"urn": response_data["urn"]})
    expected_response["experiment"].update(
        {
            "urn": experiment["urn"],
            "experimentSetUrn": experiment["experimentSetUrn"],
            "scoreSetUrns": [response_data["urn"]],
        }
    )
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
    expected_response = deepcopy(TEST_MINIMAL_SCORE_SET_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
        }
    )
    expected_response["experiment"].update(
        {
            "urn": response_data["experiment"]["urn"],
            "experimentSetUrn": response_data["experiment"]["experimentSetUrn"],
            "scoreSetUrns": [response_data["urn"]],
            "publishedDate": date.today().isoformat(),
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_publish_multiple_score_sets(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )
    score_set_3 = create_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 3"}
    )
    pub_score_set_1_response = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish")
    assert pub_score_set_1_response.status_code == 200
    pub_score_set_2_response = client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish")
    assert pub_score_set_2_response.status_code == 200
    pub_score_set_3_response = client.post(f"/api/v1/score-sets/{score_set_3['urn']}/publish")
    assert pub_score_set_3_response.status_code == 200
    pub_score_set_1_data = pub_score_set_1_response.json()
    pub_score_set_2_data = pub_score_set_2_response.json()
    pub_score_set_3_data = pub_score_set_3_response.json()
    assert pub_score_set_1_data["urn"] == "urn:mavedb:00000001-a-1"
    assert pub_score_set_1_data["title"] == score_set_1["title"]
    assert pub_score_set_1_data["experiment"]["urn"] == "urn:mavedb:00000001-a"
    assert pub_score_set_2_data["urn"] == "urn:mavedb:00000001-a-2"
    assert pub_score_set_2_data["title"] == score_set_2["title"]
    assert pub_score_set_2_data["experiment"]["urn"] == "urn:mavedb:00000001-a"
    assert pub_score_set_3_data["urn"] == "urn:mavedb:00000001-a-3"
    assert pub_score_set_3_data["title"] == score_set_3["title"]
    assert pub_score_set_3_data["experiment"]["urn"] == "urn:mavedb:00000001-a"


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


def test_create_single_score_set_meta_analysis(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    meta_score_set = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]},
    )
    score_set_refresh = client.get(f"/api/v1/score-sets/{score_set['urn']}").json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == [score_set["urn"]]
    assert score_set_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_publish_single_score_set_meta_analysis(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    meta_score_set = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]},
    )
    meta_score_set = client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish").json()
    assert meta_score_set["urn"] == f"urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_single_experiment(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )
    score_set_1 = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    score_set_2 = client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish").json()
    meta_score_set = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = client.get(f"/api/v1/score-sets/{score_set_1['urn']}").json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    meta_score_set = client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish").json()
    assert meta_score_set["urn"] == f"urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets(client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1 = create_score_set_with_variants(
        client, experiment_1["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_score_set_with_variants(
        client, experiment_2["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )
    score_set_1 = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    score_set_2 = client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish").json()
    meta_score_set = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = client.get(f"/api/v1/score-sets/{score_set_1['urn']}").json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    meta_score_set = client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish").json()
    assert meta_score_set["urn"] == f"urn:mavedb:00000003-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiments(client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(
        client, {"title": "Experiment 2", "experimentSetUrn": experiment_1["experimentSetUrn"]}
    )
    score_set_1 = create_score_set_with_variants(
        client, experiment_1["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_score_set_with_variants(
        client, experiment_2["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )
    score_set_1 = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    score_set_2 = client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish").json()
    meta_score_set = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = client.get(f"/api/v1/score-sets/{score_set_1['urn']}").json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    meta_score_set = client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish").json()
    assert meta_score_set["urn"] == f"urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets_different_score_sets(
    client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1_1 = create_score_set_with_variants(
        client, experiment_1["urn"], data_files / "scores.csv", update={"title": "Exp 1 Score Set 1"}
    )
    score_set_1_2 = create_score_set_with_variants(
        client, experiment_1["urn"], data_files / "scores.csv", update={"title": "Exp 1 Score Set 2"}
    )
    score_set_2_1 = create_score_set_with_variants(
        client, experiment_2["urn"], data_files / "scores.csv", update={"title": "Exp 2 Score Set 1"}
    )
    score_set_2_2 = create_score_set_with_variants(
        client, experiment_2["urn"], data_files / "scores.csv", update={"title": "Exp 2 Score Set 2"}
    )
    score_set_1_1 = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish").json()
    score_set_1_2 = client.post(f"/api/v1/score-sets/{score_set_1_2['urn']}/publish").json()
    score_set_2_1 = client.post(f"/api/v1/score-sets/{score_set_2_1['urn']}/publish").json()
    score_set_2_2 = client.post(f"/api/v1/score-sets/{score_set_2_2['urn']}/publish").json()
    meta_score_set_1 = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-1 2-1",
            "metaAnalyzesScoreSetUrns": [score_set_1_1["urn"], score_set_2_1["urn"]],
        },
    )
    score_set_1_1_refresh = client.get(f"/api/v1/score-sets/{score_set_1_1['urn']}").json()
    assert meta_score_set_1["metaAnalyzesScoreSetUrns"] == sorted([score_set_1_1["urn"], score_set_2_1["urn"]])
    assert score_set_1_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set_1["urn"]]
    meta_score_set_1 = client.post(f"/api/v1/score-sets/{meta_score_set_1['urn']}/publish").json()
    assert meta_score_set_1["urn"] == f"urn:mavedb:00000003-0-1"
    meta_score_set_2 = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-2 2-2",
            "metaAnalyzesScoreSetUrns": [score_set_1_2["urn"], score_set_2_2["urn"]],
        },
    )
    meta_score_set_2 = client.post(f"/api/v1/score-sets/{meta_score_set_2['urn']}/publish").json()
    assert meta_score_set_2["urn"] == f"urn:mavedb:00000003-0-2"
    meta_score_set_3 = create_score_set_with_variants(
        client,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-1 2-2",
            "metaAnalyzesScoreSetUrns": [score_set_1_1["urn"], score_set_2_2["urn"]],
        },
    )
    meta_score_set_3 = client.post(f"/api/v1/score-sets/{meta_score_set_3['urn']}/publish").json()
    assert meta_score_set_3["urn"] == f"urn:mavedb:00000003-0-3"
