from tests.helpers.constants import TEST_USER
from tests.helpers.util import (
    add_contributor,
    change_ownership,
    create_experiment,
)
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel


def test_get_true_authorization_from_own_experiment_set_check(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.get(f"/api/v1/experiment-sets/check-authorizations/{experiment['experimentSetUrn']}")

    assert response.status_code == 200
    assert response.json() == True


def test_contributor_gets_true_authorization_from_others_experiment_set_check(session, client, setup_router_db):
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
    response = client.get(f"/api/v1/experiment-sets/check-authorizations/{experiment['experimentSetUrn']}")

    assert response.status_code == 200
    assert response.json() == True


def test_get_false_authorization_from_other_users_experiment_set_check(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)

    response = client.get(f"/api/v1/experiment-sets/check-authorizations/{experiment['experimentSetUrn']}")

    assert response.status_code == 200
    assert response.json() == False