from tests.helpers.constants import TEST_USER
from tests.helpers.util import (
    add_contributor,
    change_ownership,
    create_experiment,
    create_seq_score_set,
    create_seq_score_set_with_variants,
)
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel


# Test check_authorization function
# Experiment set tests
def test_get_true_permission_from_own_experiment_set_add_experiment_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_permission_from_others_experiment_set_add_experiment_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_permission_from_others_experiment_set_add_experiment_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/add_experiment")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_permission_with_wrong_action_in_experiment_set(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/{experiment['experimentSetUrn']}/edit")

    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"][0]["msg"] == "value is not a valid enumeration member; permitted: 'read', " \
                                                "'update', 'delete', 'add_experiment', 'add_score_set', 'set_scores'," \
                                                " 'add_role', 'publish'"


def test_cannot_get_permission_with_non_existing_experiment_set(client, setup_router_db):
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment-set with URN 'invalidUrn' not found"


# Experiment tests
def test_get_true_permission_from_own_experiment_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_permission_from_own_experiment_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_permission_from_own_experiment_add_score_set_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json() == True


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
    assert response.json() == True


def test_contributor_gets_true_permission_from_others_experiment_delete_check(session, client, setup_router_db):
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
    assert response.json() == True


def test_contributor_gets_true_permission_from_others_private_experiment_add_score_set_check(session, client, setup_router_db):
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
    assert response.json() == True


def test_get_false_permission_from_others_private_experiment_add_score_set_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/add_score_set")

    assert response.status_code == 200
    assert response.json() == False


def test_get_true_permission_from_others_public_experiment_add_score_set_check(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    pub_score_set = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    pub_experiment_urn = pub_score_set["experiment"]["urn"]
    change_ownership(session, pub_experiment_urn, ExperimentDbModel)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{pub_experiment_urn}/add_score_set")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_permission_from_others_experiment_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/update")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_permission_from_other_users_experiment_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_permission_with_wrong_action_in_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/{experiment['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"][0]["msg"] == "value is not a valid enumeration member; permitted: 'read', " \
                                                "'update', 'delete', 'add_experiment', 'add_score_set', 'set_scores'," \
                                                " 'add_role', 'publish'"


def test_cannot_get_permission_with_non_existing_experiment(client, setup_router_db):
    response = client.get(f"/api/v1/permissions/user-is-permitted/experiment/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "experiment with URN 'invalidUrn' not found"


# Score set tests
def test_get_true_permission_from_own_score_set_update_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_permission_from_own_score_set_delete_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == True


def test_get_true_permission_from_own_score_set_publish_check(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json() == True


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
    assert response.json() == True


def test_contributor_gets_true_permission_from_others_score_set_delete_check(session, client, setup_router_db):
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
    assert response.json() == True


def test_contributor_gets_true_permission_from_others_score_set_publish_check(session, client, setup_router_db):
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
    assert response.json() == True


def test_get_false_permission_from_others_score_set_delete_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/delete")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_permission_from_others_score_set_update_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/update")

    assert response.status_code == 200
    assert response.json() == False


def test_get_false_permission_from_others_score_set_publish_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/publish")

    assert response.status_code == 200
    assert response.json() == False


def test_cannot_get_permission_with_wrong_action_in_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/{score_set['urn']}/invalidAction")

    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"][0]["msg"] == "value is not a valid enumeration member; permitted: 'read', " \
                                                "'update', 'delete', 'add_experiment', 'add_score_set', 'set_scores'," \
                                                " 'add_role', 'publish'"


def test_cannot_get_permission_with_non_existing_experiment(client, setup_router_db):
    response = client.get(f"/api/v1/permissions/user-is-permitted/score-set/invalidUrn/update")

    assert response.status_code == 404
    response_data = response.json()
    assert response_data["detail"] == "score-set with URN 'invalidUrn' not found"


# Common invalid test
def test_cannot_get_permission_with_non_existing_item(client, setup_router_db):
    response = client.get(f"/api/v1/permissions/user-is-permitted/invalidModel/invalidUrn/update")

    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"][0]["msg"] == "value is not a valid enumeration member; permitted: " \
                                                "'experiment', 'experiment-set', 'score-set'"
