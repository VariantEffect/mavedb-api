# ruff: noqa: E402

import re
from copy import deepcopy

import jsonschema
import pytest
from sqlalchemy import select

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.validation.urn_re import MAVEDB_COLLECTION_URN_RE
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.view_models.collection import Collection
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_COLLECTION,
    TEST_COLLECTION_RESPONSE,
    TEST_USER,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.collection import create_collection
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set
from tests.helpers.util.variant import mock_worker_variant_insertion


def test_create_private_collection(client, setup_router_db):
    response = client.post("/api/v1/collections/", json=TEST_COLLECTION)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Collection.model_json_schema())
    assert isinstance(MAVEDB_COLLECTION_URN_RE.fullmatch(response_data["urn"]), re.Match)
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update({"urn": response_data["urn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_create_public_collection(client, setup_router_db):
    collection = deepcopy(TEST_COLLECTION)
    collection["private"] = False
    response = client.post("/api/v1/collections/", json=collection)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Collection.model_json_schema())
    assert isinstance(MAVEDB_COLLECTION_URN_RE.fullmatch(response_data["urn"]), re.Match)
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "private": False})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


@pytest.mark.parametrize("role", ContributionRole._member_names_)
def test_add_collection_user_to_collection_role(role, client, setup_router_db):
    collection = create_collection(client, {"private": True})

    response = client.post(
        f"/api/v1/collections/{collection['urn']}/{role}s", json={"orcid_id": EXTRA_USER["username"]}
    )
    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": collection["urn"],
            "badgeName": None,
            "description": None,
        }
    )
    expected_response[f"{role}s"].extend(
        [
            {
                "recordType": "User",
                "firstName": EXTRA_USER["first_name"],
                "lastName": EXTRA_USER["last_name"],
                "orcidId": EXTRA_USER["username"],
            },
        ]
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_creator_can_read_private_collection(session, client, setup_router_db):
    collection = create_collection(client)

    response = client.get(f"/api/v1/collections/{collection['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update({"urn": response_data["urn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_admin_can_read_private_collection(session, client, setup_router_db, extra_user_app_overrides):
    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/admins", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
            "admins": [
                {
                    "recordType": "User",
                    "firstName": TEST_USER["first_name"],
                    "lastName": TEST_USER["last_name"],
                    "orcidId": TEST_USER["username"],
                },
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                },
            ],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_editor_can_read_private_collection(session, client, setup_router_db, extra_user_app_overrides):
    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/editors", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
            "editors": [
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                }
            ],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_viewer_can_read_private_collection(session, client, setup_router_db, extra_user_app_overrides):
    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
            "viewers": [
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                }
            ],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_unauthorized_user_cannot_read_private_collection(session, client, setup_router_db, extra_user_app_overrides):
    collection = create_collection(client)

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 404
    assert f"collection with URN '{collection['urn']}' not found" in response.json()["detail"]


def test_anonymous_cannot_read_private_collection(session, client, setup_router_db, anonymous_app_overrides):
    collection = create_collection(client)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 404
    assert f"collection with URN '{collection['urn']}' not found" in response.json()["detail"]


def test_anonymous_can_read_public_collection(session, client, setup_router_db, anonymous_app_overrides):
    collection = create_collection(client, {"private": False})

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "private": False})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_admin_can_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/admins", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": collection["urn"],
            "badgeName": None,
            "description": None,
            "modifiedBy": {
                "recordType": "User",
                "firstName": EXTRA_USER["first_name"],
                "lastName": EXTRA_USER["last_name"],
                "orcidId": EXTRA_USER["username"],
            },
            "admins": [
                {
                    "recordType": "User",
                    "firstName": TEST_USER["first_name"],
                    "lastName": TEST_USER["last_name"],
                    "orcidId": TEST_USER["username"],
                },
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                },
            ],
            "experimentUrns": [score_set["experiment"]["urn"]],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_editor_can_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/editors", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": collection["urn"],
            "badgeName": None,
            "description": None,
            "modifiedBy": {
                "recordType": "User",
                "firstName": EXTRA_USER["first_name"],
                "lastName": EXTRA_USER["last_name"],
                "orcidId": EXTRA_USER["username"],
            },
            "editors": [
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                }
            ],
            "experimentUrns": [score_set["experiment"]["urn"]],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_viewer_cannot_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions for URN '{collection['urn']}'" in response_data["detail"]


def test_unauthorized_user_cannot_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 404
    assert f"collection with URN '{collection['urn']}' not found" in response.json()["detail"]


def test_anonymous_cannot_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]


def test_admin_can_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/admins", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": collection["urn"],
            "badgeName": None,
            "description": None,
            "modifiedBy": {
                "recordType": "User",
                "firstName": EXTRA_USER["first_name"],
                "lastName": EXTRA_USER["last_name"],
                "orcidId": EXTRA_USER["username"],
            },
            "admins": [
                {
                    "recordType": "User",
                    "firstName": TEST_USER["first_name"],
                    "lastName": TEST_USER["last_name"],
                    "orcidId": TEST_USER["username"],
                },
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                },
            ],
            "scoreSetUrns": [score_set["urn"]],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_editor_can_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/editors", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 200
    response_data = response.json()
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": collection["urn"],
            "badgeName": None,
            "description": None,
            "modifiedBy": {
                "recordType": "User",
                "firstName": EXTRA_USER["first_name"],
                "lastName": EXTRA_USER["last_name"],
                "orcidId": EXTRA_USER["username"],
            },
            "editors": [
                {
                    "recordType": "User",
                    "firstName": EXTRA_USER["first_name"],
                    "lastName": EXTRA_USER["last_name"],
                    "orcidId": EXTRA_USER["username"],
                }
            ],
            "scoreSetUrns": [score_set["urn"]],
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_viewer_cannot_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions for URN '{collection['urn']}'" in response_data["detail"]


def test_unauthorized_user_cannot_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 404
    assert f"collection with URN '{collection['urn']}' not found" in response.json()["detail"]


def test_anonymous_cannot_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    score_set_id = session.scalars(
        select(ScoreSetDbModel.id).where(ScoreSetDbModel.urn == unpublished_score_set["urn"])
    ).one()
    score_set = publish_score_set(client, unpublished_score_set["urn"], score_set_id)

    collection = create_collection(client)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]
