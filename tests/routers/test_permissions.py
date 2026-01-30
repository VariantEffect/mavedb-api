# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from unittest.mock import patch

from mavedb.lib.permissions import Action
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_calibration import ScoreCalibration as ScoreCalibrationDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.constants import EXTRA_USER, TEST_MINIMAL_CALIBRATION, TEST_USER
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import create_test_score_calibration_in_score_set_via_client
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set
from tests.helpers.util.user import change_ownership
from tests.helpers.util.variant import mock_worker_variant_insertion


# Test check_authorization function
# Experiment set tests
def test_get_true_permission_from_own_experiment_set_add_experiment_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_experiment_set_add_experiment_check(
    session, client, setup_router_db
):
    experiment = create_experiment(client)
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
    response = client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_false_permission_from_others_experiment_set_add_experiment_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)

    response = client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    assert response.status_code == 200
    assert not response.json()


def test_cannot_get_permission_with_wrong_action_in_experiment_set(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/edit")

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_experiment_set(client, setup_router_db):
    response = client.get("/api/v1/permissions/user-is-permitted/experiment-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment-set with URN 'invalidUrn' not found"


# Experiment tests
def test_get_true_permission_from_own_experiment_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_experiment_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_experiment_add_score_set_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_experiment_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_experiment_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_contributor_gets_true_permission_from_others_private_experiment_add_score_set_check(
    session, client, setup_router_db
):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json()


def test_get_false_permission_from_others_private_experiment_add_score_set_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert not response.json()


def test_get_true_permission_from_others_public_experiment_add_score_set_check(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    published_experiment_urn = published_score_set["experiment"]["urn"]
    change_ownership(session, published_experiment_urn, ExperimentDbModel)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{published_experiment_urn}/add_score_set")

    assert response.status_code == 200
    assert response.json()


def test_get_false_permission_from_others_experiment_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert not response.json()


def test_get_false_permission_from_other_users_experiment_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_cannot_get_permission_with_wrong_action_in_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_experiment(client, setup_router_db):
    response = client.get("/api/v1/permissions/user-is-permitted/experiment/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment with URN 'invalidUrn' not found"


# Score set tests
def test_get_true_permission_from_own_score_set_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_set_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_set_publish_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_score_set_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_score_set_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_contributor_gets_false_permission_from_others_score_set_publish_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert not response.json()


def test_get_false_permission_from_others_score_set_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_get_false_permission_from_others_score_set_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert not response.json()


def test_get_false_permission_from_others_score_set_publish_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert not response.json()


def test_cannot_get_permission_with_wrong_action_in_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_score_set(client, setup_router_db):
    response = client.get("/api/v1/permissions/user-is-permitted/score-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "score-set with URN 'invalidUrn' not found"


# score calibrations
# non-exhaustive, see TODO#543


def test_get_true_permission_from_own_score_calibration_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_calibration_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_investigator_provided_score_calibration_update_check(
    session, client, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )
    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(
            f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update"
        )

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_investigator_provided_score_calibration_delete_check(
    session, client, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )
    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(
            f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete"
        )

    assert response.status_code == 200
    assert not response.json()


def test_get_true_permission_as_score_set_owner_on_others_investigator_provided_score_calibration_update_check(
    session, client, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    change_ownership(session, score_calibration["urn"], ScoreCalibrationDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_false_permission_as_score_set_owner_on_others_community_score_calibration_update_check(
    session, client, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    with DependencyOverrider(admin_app_overrides):
        score_calibration = create_test_score_calibration_in_score_set_via_client(
            client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
        )

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update")

    assert response.status_code == 200
    assert not response.json()


def test_get_false_permission_from_others_score_calibration_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    change_ownership(session, score_calibration["urn"], ScoreCalibrationDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


# Common invalid test
def test_cannot_get_permission_with_non_existing_item(client, setup_router_db):
    response = client.get("/api/v1/permissions/user-is-permitted/invalidModel/invalidUrn/update")

    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"][0]["msg"]
        == "Input should be 'collection', 'experiment', 'experiment-set', 'score-set' or 'score-calibration'"
    )
