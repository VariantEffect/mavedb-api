# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from unittest.mock import patch

from mavedb.lib.permissions import Action
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.constants import TEST_MINIMAL_CALIBRATION
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import create_test_score_calibration_in_score_set_via_client
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set
from tests.helpers.util.variant import mock_worker_variant_insertion


# Test check_authorization function
# Experiment set tests
def test_get_true_permission_from_own_experiment_set_add_experiment_check(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_experiment_set_add_experiment_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    contributor_client = make_contributor(experiment["experimentSetUrn"], ExperimentSetDbModel)

    response = contributor_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_not_found_from_others_experiment_set_add_experiment_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)

    response = outsider_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment"
    )

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_cannot_get_permission_with_wrong_action_in_experiment_set(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/edit"
    )

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_experiment_set(owner_client):
    response = owner_client.get("/api/v1/permissions/user-is-permitted/experiment-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment-set with URN 'invalidUrn' not found"


# Experiment tests
def test_get_true_permission_from_own_experiment_update_check(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_experiment_delete_check(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_experiment_add_score_set_check(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_experiment_update_check(owner_client, make_contributor):
    experiment = create_experiment(owner_client)
    contributor_client = make_contributor(experiment["urn"], ExperimentDbModel)

    response = contributor_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_experiment_delete_check(owner_client, make_contributor):
    experiment = create_experiment(owner_client)
    contributor_client = make_contributor(experiment["urn"], ExperimentDbModel)

    response = contributor_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_contributor_gets_true_permission_from_others_private_experiment_add_score_set_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    contributor_client = make_contributor(experiment["urn"], ExperimentDbModel)

    response = contributor_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_not_found_from_others_private_experiment_add_score_set_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)

    response = outsider_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set"
    )

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_get_true_permission_from_others_public_experiment_add_score_set_check(
    owner_client, outsider_client, session, data_provider, data_files
):
    experiment = create_experiment(owner_client)
    unpublished_score_set = create_seq_score_set(owner_client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        owner_client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(owner_client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    published_experiment_urn = published_score_set["experiment"]["urn"]

    response = outsider_client.get(
        f"/api/v1/permissions/user-is-permitted/experiment/{published_experiment_urn}/add_score_set"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_not_found_from_others_experiment_update_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)

    response = outsider_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_get_not_found_from_other_users_experiment_delete_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)

    response = outsider_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_cannot_get_permission_with_wrong_action_in_experiment(owner_client):
    experiment = create_experiment(owner_client)
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_experiment(owner_client):
    response = owner_client.get("/api/v1/permissions/user-is-permitted/experiment/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment with URN 'invalidUrn' not found"


# Score set tests
def test_get_true_permission_from_own_score_set_update_check(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_set_delete_check(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_set_publish_check(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_score_set_update_check(owner_client, make_contributor):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)

    response = contributor_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_score_set_delete_check(owner_client, make_contributor):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)

    response = contributor_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert not response.json()


def test_contributor_gets_false_permission_from_others_score_set_publish_check(owner_client, make_contributor):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)

    response = contributor_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert not response.json()


def test_get_not_found_from_others_score_set_delete_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])

    response = outsider_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_get_not_found_from_others_score_set_update_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])

    response = outsider_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_get_not_found_from_others_score_set_publish_check(owner_client, outsider_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])

    response = outsider_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_cannot_get_permission_with_wrong_action_in_score_set(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    response = owner_client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(action in response_data["detail"][0]["msg"] for action in (a.value for a in Action))


def test_cannot_get_permission_with_non_existing_score_set(owner_client):
    response = owner_client.get("/api/v1/permissions/user-is-permitted/score-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "score-set with URN 'invalidUrn' not found"


# score calibrations
# non-exhaustive; tests/lib/permissions covers the rules directly


def test_get_true_permission_from_own_score_calibration_update_check(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        owner_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_true_permission_from_own_score_calibration_delete_check(owner_client):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        owner_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete"
    )

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_true_permission_from_others_investigator_provided_score_calibration_update_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        owner_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)

    response = contributor_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update"
    )

    assert response.status_code == 200
    assert response.json()


def test_contributor_gets_false_permission_from_others_investigator_provided_score_calibration_delete_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        owner_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)

    response = contributor_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete"
    )

    assert response.status_code == 200
    assert not response.json()


def test_get_true_permission_as_score_set_owner_on_others_investigator_provided_score_calibration_update_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        contributor_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    assert score_calibration["investigatorProvided"] is True

    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update"
    )

    assert response.status_code == 200
    assert response.json()


def test_get_not_found_as_score_set_owner_on_others_community_score_calibration_update_check(
    owner_client, admin_client
):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        admin_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    assert score_calibration["investigatorProvided"] is False

    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/update"
    )

    # Unreadable resources are reported as missing, so the check can't confirm they exist.
    assert response.status_code == 404


def test_get_false_permission_as_score_set_owner_on_others_investigator_provided_score_calibration_delete_check(
    owner_client, make_contributor
):
    experiment = create_experiment(owner_client)
    score_set = create_seq_score_set(owner_client, experiment["urn"])
    contributor_client = make_contributor(score_set["urn"], ScoreSetDbModel)
    score_calibration = create_test_score_calibration_in_score_set_via_client(
        contributor_client, score_set["urn"], deepcamelize(TEST_MINIMAL_CALIBRATION)
    )
    assert score_calibration["investigatorProvided"] is True

    response = owner_client.get(
        f"/api/v1/permissions/user-is-permitted/score-calibration/{score_calibration['urn']}/delete"
    )

    assert response.status_code == 200
    assert not response.json()


# Common invalid test
def test_cannot_get_permission_with_non_existing_item(owner_client):
    response = owner_client.get("/api/v1/permissions/user-is-permitted/invalidModel/invalidUrn/update")

    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"][0]["msg"]
        == "Input should be 'collection', 'experiment', 'experiment-set', 'score-set' or 'score-calibration'"
    )
