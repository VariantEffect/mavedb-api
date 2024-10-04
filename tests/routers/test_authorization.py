from tests.helpers.constants import TEST_USER
from tests.helpers.util import (
    add_contributor,
    change_ownership,
    create_experiment,
    create_seq_score_set,
)
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel


# Test check_authorization function
# Experiment set tests
def test_get_true_authorization_from_own_experiment_set_add_experiment_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_experiment_set_add_experiment_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_authorization_from_other_users_experiment_set_add_experiment_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)

    response = client.get(f"/api/v1/user-is-authorized/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_authorization_with_wrong_action_in_experiment_set(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment-set/{experiment['experimentSetUrn']}/edit")

    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] == "Invalid action: edit"


def test_cannot_get_authorization_with_non_existing_experiment_set(client, setup_router_db):
    response = client.get(f"/api/v1/user-is-authorized/experiment-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment-set with URN 'invalidUrn' not found"


# Experiment tests
def test_get_true_authorization_from_own_experiment_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_authorization_from_own_experiment_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_authorization_from_own_experiment_add_score_set_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_experiment_update_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_experiment_delete_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_experiment_add_score_set_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_authorization_from_other_users_experiment_add_score_set_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_authorization_from_other_users_experiment_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_authorization_from_other_users_experiment_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_authorization_with_wrong_action_in_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/user-is-authorized/experiment/{experiment['urn']}/invalidAction")

    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] == "Invalid action: invalidAction"


def test_cannot_get_authorization_with_non_existing_experiment(client, setup_router_db):
    response = client.get(f"/api/v1/user-is-authorized/experiment/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment with URN 'invalidUrn' not found"


# Score set tests
def test_get_true_authorization_from_own_score_set_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_authorization_from_own_score_set_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_authorization_from_own_score_set_publish_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_score_set_update_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_score_set_delete_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_score_set_publish_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_authorization_from_other_users_score_set_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_authorization_from_other_users_score_set_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_authorization_from_other_users_score_set_publish_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_authorization_with_wrong_action_in_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/user-is-authorized/score-set/{score_set['urn']}/invalidAction")

    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] == "Invalid action: invalidAction"


def test_cannot_get_authorization_with_non_existing_experiment(client, setup_router_db):
    response = client.get(f"/api/v1/user-is-authorized/score-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "score-set with URN 'invalidUrn' not found"


# Common invalid test
def test_cannot_get_authorization_with_non_existing_item(client, setup_router_db):
    response = client.get(f"/api/v1/user-is-authorized/invalidModel/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "invalidModel with URN 'invalidUrn' not found"
