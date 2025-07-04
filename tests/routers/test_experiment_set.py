# ruff: noqa: E402

import pytest
from unittest.mock import patch

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel

from tests.helpers.constants import (
    TEST_USER,
)
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set_with_variants, publish_score_set
from tests.helpers.util.user import change_ownership


def test_users_get_one_private_experiment_from_own_experiment_set(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/experiment-sets/{experiment['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == experiment["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 0


def test_users_get_one_experiment_one_score_set_from_own_private_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    response = client.get(f"/api/v1/experiment-sets/{experiment['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == experiment["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 1
    assert score_set["urn"] in response_data["experiments"][0]["scoreSetUrns"]


def test_users_get_one_experiment_one_score_set_from_others_private_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    add_contributor(
        session,
        experiment["experimentSetUrn"],
        ExperimentSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/experiment-sets/{experiment['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == experiment["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 1
    assert score_set["urn"] in response_data["experiments"][0]["scoreSetUrns"]


def test_users_get_one_experiment_none_score_set_from_others_private_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    add_contributor(
        session,
        experiment["experimentSetUrn"],
        ExperimentSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/experiment-sets/{experiment['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == experiment["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 0


def test_users_get_none_experiment_none_score_set_from_others_private_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    add_contributor(
        session,
        experiment["experimentSetUrn"],
        ExperimentSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/experiment-sets/{experiment['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 0


def test_users_get_one_experiment_one_score_set_from_own_public_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        pub_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/experiment-sets/{pub_score_set['experiment']['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == pub_score_set["experiment"]["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 1
    assert pub_score_set["urn"] in response_data["experiments"][0]["scoreSetUrns"]


def test_users_get_one_experiment_one_score_set_from_other_public_experiment_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        pub_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, pub_score_set["urn"], ScoreSetDbModel)
    change_ownership(session, pub_score_set["experiment"]["urn"], ExperimentDbModel)
    change_ownership(session, pub_score_set["experiment"]["experimentSetUrn"], ExperimentSetDbModel)
    response = client.get(f"/api/v1/experiment-sets/{pub_score_set['experiment']['experimentSetUrn']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["numExperiments"] == 1
    assert response_data["experiments"][0]["urn"] == pub_score_set["experiment"]["urn"]
    assert response_data["experiments"][0]["numScoreSets"] == 1
    assert pub_score_set["urn"] in response_data["experiments"][0]["scoreSetUrns"]
