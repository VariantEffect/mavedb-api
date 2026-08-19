# ruff: noqa: E402

import re
from copy import deepcopy
from unittest.mock import patch

import jsonschema
import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.validation.urn_re import MAVEDB_COLLECTION_URN_RE
from mavedb.models.collection_experiment_association import CollectionExperimentAssociation
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.view_models.collection import Collection
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_COLLECTION,
    TEST_COLLECTION_RESPONSE,
    ADMIN_USER,
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
    # Only callers who may add users see the full roster, so EXTRA_USER does not appear
    # in "editors" here -- TEST_COLLECTION_RESPONSE's empty default is correct.
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
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
    # Only callers who may add users see the full roster, so EXTRA_USER does not appear
    # in "viewers" here -- TEST_COLLECTION_RESPONSE's empty default is correct.
    expected_response = deepcopy(TEST_COLLECTION_RESPONSE)
    expected_response.update(
        {
            "urn": response_data["urn"],
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
    assert f"collection with URN '{collection['urn']}'" in response.json()["detail"]


def test_anonymous_cannot_read_private_collection(session, client, setup_router_db, anonymous_app_overrides):
    collection = create_collection(client)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 404
    assert f"collection with URN '{collection['urn']}'" in response.json()["detail"]


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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/editors", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 200
    response_data = response.json()
    # Only callers who may add users see the full roster, so EXTRA_USER does not appear
    # in "editors" here -- TEST_COLLECTION_RESPONSE's empty default is correct.
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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/experiments",
            json={"experiment_urn": score_set["experiment"]["urn"]},
        )

    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions on collection with URN '{collection['urn']}'" in response_data["detail"]


def test_unauthorized_user_cannot_add_experiment_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/editors", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 200
    response_data = response.json()
    # Only callers who may add users see the full roster, so EXTRA_USER does not appear
    # in "editors" here -- TEST_COLLECTION_RESPONSE's empty default is correct.
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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    collection = create_collection(client)
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions on collection with URN '{collection['urn']}'" in response_data["detail"]


def test_unauthorized_user_cannot_add_score_set_to_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    collection = create_collection(client)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_set["urn"]}
        )

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]


# ========== Ordering Tests ==========


def test_create_collection_preserves_score_set_order(session, client, data_provider, data_files, setup_router_db):
    """Test that creating a collection with multiple score sets preserves their order."""
    # Create three score sets
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with specific order: [2, 0, 1]
    ordered_urns = [score_sets[2]["urn"], score_sets[0]["urn"], score_sets[1]["urn"]]
    collection = create_collection(client, {"score_set_urns": ordered_urns})

    # Verify order is preserved
    assert collection["scoreSetUrns"] == ordered_urns

    # Fetch and verify order persists
    response = client.get(f"/api/v1/collections/{collection['urn']}")
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == ordered_urns


def test_create_collection_preserves_experiment_order(session, client, data_provider, data_files, setup_router_db):
    """Test that creating a collection with multiple experiments preserves their order."""
    # Create three experiments with published score sets
    experiments = []
    for i in range(3):
        experiment = create_experiment(client)
        unpublished = create_seq_score_set(client, experiment["urn"])
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        experiment["urn"] = published["experiment"]["urn"]  # Update to published experiment URN
        experiments.append(experiment)

    # Create collection with specific order: [2, 0, 1]
    ordered_urns = [experiments[2]["urn"], experiments[0]["urn"], experiments[1]["urn"]]
    collection = create_collection(client, {"experiment_urns": ordered_urns})

    # Verify order is preserved
    assert collection["experimentUrns"] == ordered_urns

    # Fetch and verify order persists
    response = client.get(f"/api/v1/collections/{collection['urn']}")
    assert response.status_code == 200
    assert response.json()["experimentUrns"] == ordered_urns


def test_update_collection_reorders_score_sets(session, client, data_provider, data_files, setup_router_db):
    """Test reordering score sets via PATCH."""
    # Create collection with three score sets in order [A, B, C]
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    original_order = [ss["urn"] for ss in score_sets]
    collection = create_collection(client, {"score_set_urns": original_order})
    assert collection["scoreSetUrns"] == original_order

    # Reorder to [C, A, B]
    new_order = [score_sets[2]["urn"], score_sets[0]["urn"], score_sets[1]["urn"]]
    response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": new_order})
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == new_order

    # Verify persistence
    response = client.get(f"/api/v1/collections/{collection['urn']}")
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == new_order


def test_update_collection_reorders_experiments(session, client, data_provider, data_files, setup_router_db):
    """Test reordering experiments via PATCH."""
    # Create collection with three experiments
    experiments = []
    for i in range(3):
        experiment = create_experiment(client)
        unpublished = create_seq_score_set(client, experiment["urn"])
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        experiment["urn"] = published["experiment"]["urn"]  # Update to published experiment URN
        experiments.append(experiment)

    original_order = [e["urn"] for e in experiments]
    collection = create_collection(client, {"experiment_urns": original_order})
    assert collection["experimentUrns"] == original_order

    # Reorder to [C, A, B]
    new_order = [experiments[2]["urn"], experiments[0]["urn"], experiments[1]["urn"]]
    response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"experiment_urns": new_order})
    assert response.status_code == 200
    assert response.json()["experimentUrns"] == new_order


def test_add_score_set_appends_to_end(session, client, data_provider, data_files, setup_router_db):
    """Test that adding a score set via POST appends to the end."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with first two
    initial_urns = [score_sets[0]["urn"], score_sets[1]["urn"]]
    collection = create_collection(client, {"score_set_urns": initial_urns})

    # Add third via POST
    response = client.post(
        f"/api/v1/collections/{collection['urn']}/score-sets", json={"score_set_urn": score_sets[2]["urn"]}
    )
    assert response.status_code == 200

    # Verify it's appended at the end
    expected_order = initial_urns + [score_sets[2]["urn"]]
    assert response.json()["scoreSetUrns"] == expected_order


def test_remove_score_set_preserves_remaining_order(session, client, data_provider, data_files, setup_router_db):
    """Test that removing a score set preserves the order of remaining items."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with all three [A, B, C]
    all_urns = [ss["urn"] for ss in score_sets]
    collection = create_collection(client, {"score_set_urns": all_urns})

    # Remove middle one (B)
    response = client.delete(f"/api/v1/collections/{collection['urn']}/score-sets/{score_sets[1]['urn']}")
    assert response.status_code == 200

    # Verify order is [A, C]
    expected_order = [score_sets[0]["urn"], score_sets[2]["urn"]]
    assert response.json()["scoreSetUrns"] == expected_order


def test_add_experiment_appends_to_end(session, client, data_provider, data_files, setup_router_db):
    """Test that adding an experiment via POST appends to the end."""
    experiments = []
    for i in range(3):
        experiment = create_experiment(client)
        unpublished = create_seq_score_set(client, experiment["urn"])
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        experiment["urn"] = published["experiment"]["urn"]  # Update to published experiment URN
        experiments.append(experiment)

    # Create collection with first two
    initial_urns = [experiments[0]["urn"], experiments[1]["urn"]]
    collection = create_collection(client, {"experiment_urns": initial_urns})

    # Add third via POST
    response = client.post(
        f"/api/v1/collections/{collection['urn']}/experiments", json={"experiment_urn": experiments[2]["urn"]}
    )
    assert response.status_code == 200

    # Verify it's appended at the end
    expected_order = initial_urns + [experiments[2]["urn"]]
    assert response.json()["experimentUrns"] == expected_order


def test_remove_experiment_preserves_remaining_order(session, client, data_provider, data_files, setup_router_db):
    """Test that removing an experiment preserves the order of remaining items."""
    experiments = []
    for i in range(3):
        experiment = create_experiment(client)
        unpublished = create_seq_score_set(client, experiment["urn"])
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        experiment["urn"] = published["experiment"]["urn"]  # Update to published experiment URN
        experiments.append(experiment)

    # Create collection with all three [A, B, C]
    all_urns = [e["urn"] for e in experiments]
    collection = create_collection(client, {"experiment_urns": all_urns})

    # Remove middle one (B)
    response = client.delete(f"/api/v1/collections/{collection['urn']}/experiments/{experiments[1]['urn']}")
    assert response.status_code == 200

    # Verify order is [A, C]
    expected_order = [experiments[0]["urn"], experiments[2]["urn"]]
    assert response.json()["experimentUrns"] == expected_order


def test_patch_adds_new_score_set_via_urns(session, client, data_provider, data_files, setup_router_db):
    """Test that PATCH with score_set_urns can add new score sets (implicit add)."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with first two
    initial_urns = [score_sets[0]["urn"], score_sets[1]["urn"]]
    collection = create_collection(client, {"score_set_urns": initial_urns})

    # PATCH to add third (implicit add)
    new_urns = initial_urns + [score_sets[2]["urn"]]
    response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": new_urns})
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == new_urns


def test_patch_removes_score_set_via_urns(session, client, data_provider, data_files, setup_router_db):
    """Test that PATCH with score_set_urns can remove score sets (implicit remove)."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with all three
    all_urns = [ss["urn"] for ss in score_sets]
    collection = create_collection(client, {"score_set_urns": all_urns})

    # PATCH to remove middle one (implicit remove)
    new_urns = [score_sets[0]["urn"], score_sets[2]["urn"]]
    response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": new_urns})
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == new_urns


def test_patch_reorders_and_modifies_membership_atomically(session, client, data_provider, data_files, setup_router_db):
    """Test that PATCH can simultaneously reorder, add, and remove score sets."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(3):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Create collection with first two: [A, B]
    initial_urns = [score_sets[0]["urn"], score_sets[1]["urn"]]
    collection = create_collection(client, {"score_set_urns": initial_urns})

    # PATCH to: remove B, add C, reorder to [C, A]
    new_urns = [score_sets[2]["urn"], score_sets[0]["urn"]]
    response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": new_urns})
    assert response.status_code == 200
    assert response.json()["scoreSetUrns"] == new_urns


def test_patch_with_nonexistent_urn_returns_404(client, setup_router_db):
    """Test that PATCH with a nonexistent URN returns 404."""
    collection = create_collection(client)

    response = client.patch(
        f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": ["urn:mavedb:00000000-a-1"]}
    )
    assert response.status_code == 404
    assert "No score set found with the given URN" in response.json()["detail"]


def test_viewer_cannot_reorder_collection(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    """Test that viewers get 403 when trying to reorder via PATCH."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(2):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    urns = [ss["urn"] for ss in score_sets]
    collection = create_collection(client, {"score_set_urns": urns})

    # Add extra user as viewer
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    # Try to reorder as viewer (reverse order)
    with DependencyOverrider(extra_user_app_overrides):
        response = client.patch(f"/api/v1/collections/{collection['urn']}", json={"score_set_urns": [urns[1], urns[0]]})

    assert response.status_code == 403


def test_viewer_cannot_add_via_patch(
    session, client, data_provider, data_files, setup_router_db, extra_user_app_overrides
):
    """Test that viewers get 403 when PATCH implies adding a score set."""
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]

    score_sets = []
    for i in range(2):
        unpublished = create_seq_score_set(client, experiment_urn)
        unpublished = mock_worker_variant_insertion(
            client, session, data_provider, unpublished, data_files / "scores.csv"
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, unpublished["urn"])
        score_sets.append(published)
        experiment_urn = published["experiment"]["urn"]

    # Collection with only first score set
    collection = create_collection(client, {"score_set_urns": [score_sets[0]["urn"]]})

    # Add extra user as viewer
    client.post(f"/api/v1/collections/{collection['urn']}/viewers", json={"orcid_id": EXTRA_USER["username"]})

    # Try to add second score set as viewer (membership change)
    with DependencyOverrider(extra_user_app_overrides):
        response = client.patch(
            f"/api/v1/collections/{collection['urn']}",
            json={"score_set_urns": [score_sets[0]["urn"], score_sets[1]["urn"]]},
        )

    assert response.status_code == 403


# Regression tests for collection membership disclosure.
#
# `PermissionResponse` has no `__bool__`, so `if has_permission(...)` was always true and every
# association filter and roster check in this router was inert. These tests exercise the paths
# from the outside, via response bodies, so they stay valid regardless of how filtering is
# implemented.


def test_public_collection_does_not_leak_private_member_score_set(
    session, client, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    private_score_set = create_seq_score_set(client, experiment["urn"])
    collection = create_collection(client, update={"private": False})

    response = client.post(
        f"/api/v1/collections/{collection['urn']}/score-sets",
        json={"score_set_urn": private_score_set["urn"]},
    )
    assert response.status_code == 200

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    assert private_score_set["urn"] not in response.text


def test_public_collection_does_not_leak_private_member_experiment(
    session, client, setup_router_db, anonymous_app_overrides
):
    private_experiment = create_experiment(client)
    collection = create_collection(client, update={"private": False})

    response = client.post(
        f"/api/v1/collections/{collection['urn']}/experiments",
        json={"experiment_urn": private_experiment["urn"]},
    )
    assert response.status_code == 200

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    assert private_experiment["urn"] not in response.text


def test_collection_owner_still_sees_own_private_member_score_set(session, client, setup_router_db):
    """Guards against over-filtering: entitled callers must still see their own private members."""
    experiment = create_experiment(client)
    private_score_set = create_seq_score_set(client, experiment["urn"])
    collection = create_collection(client, update={"private": False})

    client.post(
        f"/api/v1/collections/{collection['urn']}/score-sets",
        json={"score_set_urn": private_score_set["urn"]},
    )
    response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    assert private_score_set["urn"] in response.json()["scoreSetUrns"]


def test_non_member_does_not_see_collection_viewers_or_editors(
    session, client, setup_router_db, anonymous_app_overrides
):
    collection = create_collection(client, update={"private": False})
    assert (
        client.post(
            f"/api/v1/collections/{collection['urn']}/viewers",
            json={"orcid_id": EXTRA_USER["username"]},
        ).status_code
        == 200
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    body = response.json()
    assert body["viewers"] == []
    assert body["editors"] == []
    # Admins remain visible so contributors can identify who to contact.
    assert [admin["orcidId"] for admin in body["admins"]] == [TEST_USER["username"]]


def test_narrowing_associations_does_not_delete_them(session, client, setup_router_db, anonymous_app_overrides):
    """The router assigns filtered lists to `delete-orphan` collections.

    Nothing commits after that assignment today, so the staged orphan deletes are never
    persisted. This pins that invariant: a non-member read must not destroy the very
    association rows it declines to show.
    """
    experiment = create_experiment(client)
    private_score_set = create_seq_score_set(client, experiment["urn"])
    collection = create_collection(client, update={"private": False})
    client.post(
        f"/api/v1/collections/{collection['urn']}/score-sets",
        json={"score_set_urn": private_score_set["urn"]},
    )

    score_set_assocs_before = session.query(CollectionScoreSetAssociation).count()
    experiment_assocs_before = session.query(CollectionExperimentAssociation).count()
    assert score_set_assocs_before == 1

    with DependencyOverrider(anonymous_app_overrides):
        assert client.get(f"/api/v1/collections/{collection['urn']}").status_code == 200

    session.expire_all()
    assert session.query(CollectionScoreSetAssociation).count() == score_set_assocs_before
    assert session.query(CollectionExperimentAssociation).count() == experiment_assocs_before


@pytest.mark.parametrize("role", ContributionRole._member_names_)
def test_only_users_who_can_add_users_see_the_full_roster(
    role, session, client, setup_router_db, extra_user_app_overrides
):
    """Whoever may add a user to a collection may see who is in it; everyone else sees admins only.

    The counterpart to `test_non_member_does_not_see_collection_viewers_or_editors`, which covers
    outsiders. This covers members, and fails if the roster is opened up to editors and viewers
    on the strength of their being members at all.
    """
    collection = create_collection(client, update={"private": False})

    # TEST_USER is the creator and therefore an admin. Seat EXTRA_USER in the role under test, and
    # a third user as an editor, so there is a non-admin entry only admins should be able to see.
    for orcid, seat in ((EXTRA_USER["username"], f"{role}s"), (ADMIN_USER["username"], "editors")):
        assert (
            client.post(
                f"/api/v1/collections/{collection['urn']}/{seat}",
                json={"orcid_id": orcid},
            ).status_code
            == 200
        )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/collections/{collection['urn']}")

    assert response.status_code == 200
    body = response.json()
    orcids = {key: [user["orcidId"] for user in body[key]] for key in ("admins", "editors", "viewers")}

    if role == ContributionRole.admin.name:
        assert EXTRA_USER["username"] in orcids["admins"]
        assert ADMIN_USER["username"] in orcids["editors"]
    else:
        assert orcids["admins"] == [TEST_USER["username"]]
        assert orcids["editors"] == []
        assert orcids["viewers"] == []
