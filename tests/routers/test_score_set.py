import re
from copy import deepcopy
from datetime import date
from unittest.mock import patch

import jsonschema
import pytest
from arq import ArqRedis
from humps import camelize
from sqlalchemy import select

from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE, MAVEDB_SCORE_SET_URN_RE, MAVEDB_EXPERIMENT_URN_RE
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.view_models.orcid import OrcidUser
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import (
    EXTRA_USER,
    EXTRA_LICENSE,
    TEST_CROSSREF_IDENTIFIER,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET_RESPONSE,
    TEST_PUBMED_IDENTIFIER,
    TEST_ORCID_ID,
    TEST_SCORESET_RANGE,
    TEST_SAVED_SCORESET_RANGE,
    TEST_MINIMAL_ACC_SCORESET_RESPONSE,
    TEST_USER,
    TEST_INACTIVE_LICENSE,
    SAVED_DOI_IDENTIFIER,
    SAVED_EXTRA_CONTRIBUTOR,
    SAVED_PUBMED_PUBLICATION,
    SAVED_SHORT_EXTRA_LICENSE,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util import (
    add_contributor,
    change_ownership,
    change_to_inactive_license,
    create_experiment,
    create_seq_score_set,
    create_seq_score_set_with_variants,
    update_expected_response_for_created_resources,
)


########################################################################################################################
# Score set schemas
########################################################################################################################


def test_TEST_MINIMAL_SEQ_SCORESET_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_SEQ_SCORESET, schema=ScoreSetCreate.schema())


def test_TEST_MINIMAL_ACC_SCORESET_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_ACC_SCORESET, schema=ScoreSetCreate.schema())


########################################################################################################################
# Score set creation
########################################################################################################################


def test_create_minimal_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]

    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_create_score_set_with_contributor(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"contributors": [{"orcid_id": TEST_ORCID_ID}]})

    with patch(
        "mavedb.lib.orcid.fetch_orcid_user",
        lambda orcid_id: OrcidUser(orcid_id=orcid_id, given_name="ORCID", family_name="User"),
    ):
        response = client.post("/api/v1/score-sets/", json=score_set)

    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response["contributors"] = [
        {
            "recordType": "Contributor",
            "orcidId": TEST_ORCID_ID,
            "givenName": "ORCID",
            "familyName": "User",
        }
    ]

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_create_score_set_with_score_range(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"score_ranges": TEST_SCORESET_RANGE})

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response["scoreRanges"] = TEST_SAVED_SCORESET_RANGE

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_cannot_create_score_set_without_email(client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    client.put("api/v1/users/me", json={"email": None})
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] in "There must be an email address associated with your account to use this feature."


def test_cannot_create_score_set_with_invalid_target_gene_category(client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    score_set_post_payload["targetGenes"][0]["category"] = "some_invalid_target_category"
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert "value is not a valid enumeration member;" in response_data["detail"][0]["msg"]


########################################################################################################################
# Score set updating
########################################################################################################################


@pytest.mark.parametrize(
    "attribute,updated_data,expected_response_data",
    [
        ("title", "Updated Title", "Updated Title"),
        ("method_text", "Updated Method Text", "Updated Method Text"),
        ("abstract_text", "Updated Abstract Text", "Updated Abstract Text"),
        ("short_description", "Updated Abstract Text", "Updated Abstract Text"),
        ("extra_metadata", {"updated": "metadata"}, {"updated": "metadata"}),
        ("data_usage_policy", "data_usage_policy", "data_usage_policy"),
        ("contributors", [{"orcid_id": EXTRA_USER["username"]}], [SAVED_EXTRA_CONTRIBUTOR]),
        ("primary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}], [SAVED_PUBMED_PUBLICATION]),
        ("secondary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}], [SAVED_PUBMED_PUBLICATION]),
        ("doi_identifiers", [{"identifier": TEST_CROSSREF_IDENTIFIER}], [SAVED_DOI_IDENTIFIER]),
        ("license_id", EXTRA_LICENSE["id"], SAVED_SHORT_EXTRA_LICENSE),
        ("target_genes", TEST_MINIMAL_ACC_SCORESET["targetGenes"], TEST_MINIMAL_ACC_SCORESET_RESPONSE["targetGenes"]),
        ("score_ranges", TEST_SCORESET_RANGE, TEST_SAVED_SCORESET_RANGE),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_can_update_score_set_data_before_publication(
    client, setup_router_db, attribute, updated_data, expected_response_data, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    score_set_update_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_update_payload.update({camelize(attribute): updated_data})
    response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set_update_payload)
    assert response.status_code == 200

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()

    # Although the client provides the license id, the response includes the full license.
    if attribute == "license_id":
        attribute = "license"

    assert expected_response_data == response_data[camelize(attribute)]


@pytest.mark.parametrize(
    "attribute,updated_data,expected_response_data",
    [
        ("title", "Updated Title", "Updated Title"),
        ("method_text", "Updated Method Text", "Updated Method Text"),
        ("abstract_text", "Updated Abstract Text", "Updated Abstract Text"),
        ("short_description", "Updated Abstract Text", "Updated Abstract Text"),
        ("extra_metadata", {"updated": "metadata"}, {"updated": "metadata"}),
        ("data_usage_policy", "data_usage_policy", "data_usage_policy"),
        ("contributors", [{"orcid_id": EXTRA_USER["username"]}], [SAVED_EXTRA_CONTRIBUTOR]),
        ("primary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}], [SAVED_PUBMED_PUBLICATION]),
        ("secondary_publication_identifiers", [{"identifier": TEST_PUBMED_IDENTIFIER}], [SAVED_PUBMED_PUBLICATION]),
        ("doi_identifiers", [{"identifier": TEST_CROSSREF_IDENTIFIER}], [SAVED_DOI_IDENTIFIER]),
        ("license_id", EXTRA_LICENSE["id"], SAVED_SHORT_EXTRA_LICENSE),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_can_update_score_set_supporting_data_after_publication(
    session,
    data_provider,
    client,
    setup_router_db,
    attribute,
    updated_data,
    expected_response_data,
    mock_publication_fetch,
    data_files,
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 200
    published_urn = response.json()["urn"]
    response = client.get(f"/api/v1/score-sets/{published_urn}")
    assert response.status_code == 200
    response_data = response.json()

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), response_data["experiment"], response_data
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat()})
    expected_response.update(
        {
            "urn": published_urn,
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
            "processingState": ProcessingState.success.name,
        }
    )

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    score_set_update_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_update_payload.update({camelize(attribute): updated_data})
    response = client.put(f"/api/v1/score-sets/{published_urn}", json=score_set_update_payload)
    assert response.status_code == 200

    response = client.get(f"/api/v1/score-sets/{published_urn}")
    assert response.status_code == 200
    response_data = response.json()

    # Although the client provides the license id, the response includes the full license.
    if attribute == "license_id":
        attribute = "license"

    assert expected_response_data == response_data[camelize(attribute)]


@pytest.mark.parametrize(
    "attribute,updated_data,expected_response_data",
    [
        ("target_genes", TEST_MINIMAL_ACC_SCORESET["targetGenes"], TEST_MINIMAL_SEQ_SCORESET_RESPONSE["targetGenes"]),
        (
            "score_ranges",
            TEST_SCORESET_RANGE,
            None,
        ),
    ],
)
def test_cannot_update_score_set_target_data_after_publication(
    client, setup_router_db, attribute, expected_response_data, updated_data, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 200
    published_urn = response.json()["urn"]
    response = client.get(f"/api/v1/score-sets/{published_urn}")
    assert response.status_code == 200
    response_data = response.json()

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), response_data["experiment"], response_data
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat()})
    expected_response.update(
        {
            "urn": published_urn,
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
            "processingState": ProcessingState.success.name,
        }
    )

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    score_set_update_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_update_payload.update({camelize(attribute): updated_data})
    response = client.put(f"/api/v1/score-sets/{published_urn}", json=score_set_update_payload)
    assert response.status_code == 200

    response = client.get(f"/api/v1/score-sets/{published_urn}")
    assert response.status_code == 200
    response_data = response.json()

    if expected_response_data:
        assert expected_response_data == response_data[camelize(attribute)]
    else:
        assert camelize(attribute) not in response_data.keys()


########################################################################################################################
# Score set fetching
########################################################################################################################


def test_get_own_private_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_cannot_get_other_user_private_score_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_anonymous_user_cannot_get_user_private_score_set(session, client, setup_router_db, anonymous_app_overrides):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_contributor_can_get_other_users_private_score_set(session, client, setup_router_db):
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

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )
    expected_response["contributors"] = [
        {
            "recordType": "Contributor",
            "orcidId": TEST_USER["username"],
            "givenName": TEST_USER["first_name"],
            "familyName": TEST_USER["last_name"],
        }
    ]
    expected_response["createdBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    expected_response["modifiedBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_admin_can_get_other_user_private_score_set(session, client, admin_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


########################################################################################################################
# Adding scores to score set
########################################################################################################################


def test_add_score_set_variants_scores_only_endpoint(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_and_counts_endpoint(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_only_endpoint_utf8_encoded(client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores_utf8_encoded.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_and_counts_endpoint_utf8_encoded(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores_utf8_encoded.csv"
    counts_csv_path = data_files / "counts_utf8_encoded.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_cannot_add_scores_to_score_set_without_email(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    client.put("api/v1/users/me", json={"email": None})
    scores_csv_path = data_files / "scores.csv"
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] in "There must be an email address associated with your account to use this feature."


# A user should not be able to add scores to another users' score set. Therefore, they should also not be able
# to add scores and counts. So long as this test passes (a user cannot add scores to another users' score set),
# they necessarily will not be able to add scores and counts-- so omit the test for adding scores + counts.
def test_cannot_add_scores_to_other_user_score_set(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
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


# A user should not be able to add scores to another users' score set. Therefore, they should also not be able
# to add scores and counts. So long as this test passes (a user cannot add scores to another users' score set),
# they necessarily will not be able to add scores and counts-- so omit the test for adding scores + counts.
def test_anonymous_cannot_add_scores_to_other_user_score_set(
    session, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    scores_csv_path = data_files / "scores.csv"

    with open(scores_csv_path, "rb") as scores_file, DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )

    assert response.status_code == 401
    response_data = response.json()
    assert "Could not validate credentials" in response_data["detail"]


def test_contributor_can_add_scores_to_other_user_score_set(session, client, setup_router_db, data_files):
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
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    score_set["contributors"] = [
        {
            "recordType": "Contributor",
            "orcidId": TEST_USER["username"],
            "givenName": TEST_USER["first_name"],
            "familyName": TEST_USER["last_name"],
        }
    ]
    score_set["createdBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    score_set["modifiedBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    assert score_set == response_data


def test_contributor_can_add_scores_and_counts_to_other_user_score_set(session, client, setup_router_db, data_files):
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
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    score_set["contributors"] = [
        {
            "recordType": "Contributor",
            "orcidId": TEST_USER["username"],
            "givenName": TEST_USER["first_name"],
            "familyName": TEST_USER["last_name"],
        }
    ]
    score_set["createdBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    score_set["modifiedBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    assert score_set == response_data


def test_admin_can_add_scores_to_other_user_score_set(
    session, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        DependencyOverrider(admin_app_overrides),
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_admin_can_add_scores_and_counts_to_other_user_score_set(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


########################################################################################################################
# Score set publication
########################################################################################################################


def test_publish_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 200
    response_data = response.json()
    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_EXPERIMENT_URN_RE.fullmatch(response_data["experiment"]["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), response_data["experiment"], response_data
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat()})
    expected_response.update(
        {
            "urn": response_data["urn"],
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
            "processingState": ProcessingState.success.name,
        }
    )
    assert sorted(expected_response.keys()) == sorted(response_data.keys())

    # refresh score set to post worker state
    score_set = (client.get(f"/api/v1/score-sets/{response_data['urn']}")).json()
    for key in expected_response:
        assert (key, expected_response[key]) == (key, score_set[key])

    score_set_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_variants])


def test_publish_multiple_score_sets(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )
    score_set_3 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 3"}
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

    score_set_1_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_1["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_1_variants])
    score_set_2_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_2["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_2_variants])
    score_set_3_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_3["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_3_variants])


def test_cannot_publish_score_set_without_variants(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 422
    response_data = response.json()
    assert "cannot publish score set without variant scores" in response_data["detail"]


def test_cannot_publish_other_user_private_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_anonymous_cannot_publish_user_private_score_set(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")

    assert response.status_code == 401
    response_data = response.json()
    assert "Could not validate credentials" in response_data["detail"]


def test_contributor_can_publish_other_users_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )

    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["urn"] == "urn:mavedb:00000001-a-1"
    assert response_data["experiment"]["urn"] == "urn:mavedb:00000001-a"

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), response_data["experiment"], response_data
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat()})
    expected_response.update(
        {
            "urn": response_data["urn"],
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
            "processingState": ProcessingState.success.name,
        }
    )
    expected_response["contributors"] = [
        {
            "recordType": "Contributor",
            "orcidId": TEST_USER["username"],
            "givenName": TEST_USER["first_name"],
            "familyName": TEST_USER["last_name"],
        }
    ]
    expected_response["createdBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    expected_response["modifiedBy"] = {
        "recordType": "User",
        "orcidId": EXTRA_USER["username"],
        "firstName": EXTRA_USER["first_name"],
        "lastName": EXTRA_USER["last_name"],
    }
    assert sorted(expected_response.keys()) == sorted(response_data.keys())

    # refresh score set to post worker state
    score_set = (client.get(f"/api/v1/score-sets/{response_data['urn']}")).json()
    for key in expected_response:
        assert (key, expected_response[key]) == (key, score_set[key])

    score_set_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_variants])


def test_admin_cannot_publish_other_user_private_score_set(
    session, data_provider, client, admin_app_overrides, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


########################################################################################################################
# Score set meta-analysis
########################################################################################################################


def test_create_single_score_set_meta_analysis(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    score_set = (client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")).json()
    meta_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]},
    )

    score_set_refresh = (client.get(f"/api/v1/score-sets/{score_set['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == [score_set["urn"]]
    assert score_set_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_publish_single_score_set_meta_analysis(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    score_set = (client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")).json()
    meta_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]},
    )

    meta_score_set = (client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish")).json()
    assert meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_single_experiment(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )

    score_set_1 = (client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish")).json()
    score_set_2 = (client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish")).json()

    meta_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = (client.get(f"/api/v1/score-sets/{score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    meta_score_set = (client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish")).json()
    assert meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_2["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )

    score_set_1 = (client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish")).json()
    score_set_2 = (client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish")).json()

    meta_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = (client.get(f"/api/v1/score-sets/{score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    meta_score_set = (client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish")).json()
    assert meta_score_set["urn"] == "urn:mavedb:00000003-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiments(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(
        client, {"title": "Experiment 2", "experimentSetUrn": experiment_1["experimentSetUrn"]}
    )
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv", update={"title": "Score Set 1"}
    )
    score_set_2 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_2["urn"], data_files / "scores.csv", update={"title": "Score Set 2"}
    )

    score_set_1 = (client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish")).json()
    score_set_2 = (client.post(f"/api/v1/score-sets/{score_set_2['urn']}/publish")).json()

    meta_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set_1["urn"], score_set_2["urn"]]},
    )
    score_set_1_refresh = (client.get(f"/api/v1/score-sets/{score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted([score_set_1["urn"], score_set_2["urn"]])
    assert score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    meta_score_set = (client.post(f"/api/v1/score-sets/{meta_score_set['urn']}/publish")).json()
    assert meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets_different_score_sets(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Exp 1 Score Set 1"},
    )
    score_set_1_2 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Exp 1 Score Set 2"},
    )
    score_set_2_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_2["urn"],
        data_files / "scores.csv",
        update={"title": "Exp 2 Score Set 1"},
    )
    score_set_2_2 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_2["urn"],
        data_files / "scores.csv",
        update={"title": "Exp 2 Score Set 2"},
    )

    score_set_1_1 = (client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")).json()
    score_set_1_2 = (client.post(f"/api/v1/score-sets/{score_set_1_2['urn']}/publish")).json()
    score_set_2_1 = (client.post(f"/api/v1/score-sets/{score_set_2_1['urn']}/publish")).json()
    score_set_2_2 = (client.post(f"/api/v1/score-sets/{score_set_2_2['urn']}/publish")).json()
    meta_score_set_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-1 2-1",
            "metaAnalyzesScoreSetUrns": [score_set_1_1["urn"], score_set_2_1["urn"]],
        },
    )
    score_set_1_1_refresh = (client.get(f"/api/v1/score-sets/{score_set_1_1['urn']}")).json()
    assert meta_score_set_1["metaAnalyzesScoreSetUrns"] == sorted([score_set_1_1["urn"], score_set_2_1["urn"]])
    assert score_set_1_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set_1["urn"]]
    meta_score_set_2 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-2 2-2",
            "metaAnalyzesScoreSetUrns": [score_set_1_2["urn"], score_set_2_2["urn"]],
        },
    )

    meta_score_set_3 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        None,
        data_files / "scores.csv",
        update={
            "title": "Test Meta Analysis 1-1 2-2",
            "metaAnalyzesScoreSetUrns": [score_set_1_1["urn"], score_set_2_2["urn"]],
        },
    )

    meta_score_set_1 = (client.post(f"/api/v1/score-sets/{meta_score_set_1['urn']}/publish")).json()
    assert meta_score_set_1["urn"] == "urn:mavedb:00000003-0-1"
    meta_score_set_2 = (client.post(f"/api/v1/score-sets/{meta_score_set_2['urn']}/publish")).json()
    assert meta_score_set_2["urn"] == "urn:mavedb:00000003-0-2"
    meta_score_set_3 = (client.post(f"/api/v1/score-sets/{meta_score_set_3['urn']}/publish")).json()
    assert meta_score_set_3["urn"] == "urn:mavedb:00000003-0-3"


########################################################################################################################
# Score set search
########################################################################################################################


def test_search_private_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Fnord Score Set"},
    )

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == score_set_1_1["title"]


def test_search_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )

    search_payload = {"urn": score_set_1_1["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set_1_1["urn"]


# There is space in the end of test urn. The search result returned nothing before.
def test_search_private_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    urn_with_space = score_set_1_1["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set_1_1["urn"]


def test_search_others_private_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )
    change_ownership(session, score_set_1_1["urn"], ScoreSetDbModel)
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Fnord Score Set"},
    )
    change_ownership(session, score_set_1_1["urn"], ScoreSetDbModel)
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set_1_1["urn"], ScoreSetDbModel)
    search_payload = {"urn": score_set_1_1["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


# There is space in the end of test urn. The search result returned nothing before.
def test_search_others_private_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set_1_1["urn"], ScoreSetDbModel)
    urn_with_space = score_set_1_1["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_public_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    assert score_set_response.status_code == 200

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_public_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Fnord Score Set"},
    )

    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    assert score_set_response.status_code == 200
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == score_set_1_1["title"]


def test_search_public_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    published_score_set =  score_set_response.json()
    assert score_set_response.status_code == 200
    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_others_public_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Score Set"},
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    publish_score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    change_ownership(session, publish_score_set["urn"], ScoreSetDbModel)
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_public_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    score_set_1_1 = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment_1["urn"],
        data_files / "scores.csv",
        update={"title": "Test Fnord Score Set"},
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    publish_score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    change_ownership(session, publish_score_set["urn"], ScoreSetDbModel)
    assert session.query(ScoreSetDbModel).filter_by(urn=publish_score_set["urn"]).one()
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == publish_score_set["title"]


def test_search_others_public_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    publish_score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    change_ownership(session, publish_score_set["urn"], ScoreSetDbModel)
    search_payload = {"urn": score_set_1_1["urn"]}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == publish_score_set["urn"]


def test_search_others_public_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    assert score_set_response.status_code == 200
    published_score_set = score_set_response.json()
    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_private_score_sets_not_showing_public_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_1_2 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    assert score_set_response.status_code == 200
    search_payload = {"published": False}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set_1_2["urn"]


def test_search_public_score_sets_not_showing_private_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment_1 = create_experiment(client)
    score_set_1_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    create_seq_score_set_with_variants(
        client, session, data_provider, experiment_1["urn"], data_files / "scores.csv"
    )
    score_set_response = client.post(f"/api/v1/score-sets/{score_set_1_1['urn']}/publish")
    assert score_set_response.status_code == 200
    published_score_set = score_set_response.json()
    search_payload = {"published": True}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]



########################################################################################################################
# Score set deletion
########################################################################################################################


def test_anonymous_cannot_delete_other_users_private_scoreset(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]


def test_anonymous_cannot_delete_other_users_published_scoreset(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    response_data = response.json()

    with DependencyOverrider(anonymous_app_overrides):
        del_response = client.delete(f"/api/v1/score-sets/{response_data['urn']}")

    assert del_response.status_code == 401
    del_response_data = del_response.json()
    assert "Could not validate credentials" in del_response_data["detail"]


def test_can_delete_own_private_scoreset(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200


def test_cannot_delete_own_published_scoreset(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    response_data = response.json()

    del_response = client.delete(f"/api/v1/score-sets/{response_data['urn']}")

    assert del_response.status_code == 403
    del_response_data = del_response.json()
    assert f"insufficient permissions for URN '{response_data['urn']}'" in del_response_data["detail"]


def test_contributor_can_delete_other_users_private_scoreset(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )

    response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200


def test_admin_can_delete_other_users_private_scoreset(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200


def test_admin_can_delete_other_users_published_scoreset(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    response_data = response.json()

    with DependencyOverrider(admin_app_overrides):
        del_response = client.delete(f"/api/v1/score-sets/{response_data['urn']}")

    assert del_response.status_code == 200


########################################################################################################################
# Adding score sets to experiments
########################################################################################################################


def test_can_add_score_set_to_own_private_experiment(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200


def test_cannot_add_score_set_to_others_private_experiment(session, client, setup_router_db):
    experiment = create_experiment(client)
    experiment_urn = experiment["urn"]
    change_ownership(session, experiment_urn, ExperimentDbModel)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment_urn
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment_urn}' not found" in response_data["detail"]


def test_can_add_score_set_to_own_public_experiment(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    pub_score_set_1 = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    score_set_2 = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_2["experimentUrn"] = pub_score_set_1["experiment"]["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_2)
    assert response.status_code == 200


def test_can_add_score_set_to_others_public_experiment(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    pub_score_set_1 = client.post(f"/api/v1/score-sets/{score_set_1['urn']}/publish").json()
    change_ownership(session, pub_score_set_1["experiment"]["urn"], ExperimentDbModel)
    score_set_2 = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_2["experimentUrn"] = pub_score_set_1["experiment"]["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_2)
    assert response.status_code == 200


def test_contributor_can_add_score_set_to_others_private_experiment(session, client, setup_router_db):
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
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200


def test_contributor_can_add_score_set_to_others_public_experiment(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    change_ownership(session, published_score_set["experiment"]["urn"], ExperimentDbModel)
    add_contributor(
        session,
        published_score_set["experiment"]["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200


def test_cannot_create_score_set_with_inactive_license(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    score_set_post_payload["licenseId"] = TEST_INACTIVE_LICENSE["id"]
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 400


def test_cannot_modify_score_set_to_inactive_license(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set_post_payload = score_set.copy()
    score_set_post_payload.update({"licenseId": TEST_INACTIVE_LICENSE["id"], "urn": score_set["urn"]})
    response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set_post_payload)
    assert response.status_code == 400


def test_can_modify_metadata_for_score_set_with_inactive_license(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_to_inactive_license(session, score_set["urn"])
    score_set_post_payload = score_set.copy()
    score_set_post_payload.update({"title": "Update title", "urn": score_set["urn"]})
    response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    assert ("title", response_data["title"]) == ("title", "Update title")

########################################################################################################################
# Supersede score set
########################################################################################################################

def test_create_superseding_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    publish_score_set_response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    assert publish_score_set_response.status_code == 200
    published_score_set = publish_score_set_response.json()
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published_score_set["urn"]
    superseding_score_set_response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert superseding_score_set_response.status_code == 200

def test_can_view_unpublished_superseding_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    publish_score_set_response = client.post(f"/api/v1/score-sets/{unpublished_score_set['urn']}/publish")
    assert publish_score_set_response.status_code == 200
    published_score_set = publish_score_set_response.json()
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published_score_set["urn"]
    superseding_score_set_response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert superseding_score_set_response.status_code == 200
    superseding_score_set = superseding_score_set_response.json()
    score_set_response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}")
    score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    assert score_set["urn"] == superseding_score_set["supersededScoreSet"]["urn"]
    assert score_set["supersedingScoreSet"]["urn"] == superseding_score_set["urn"]

def test_cannot_view_others_unpublished_superseding_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    publish_score_set_response = client.post(f"/api/v1/score-sets/{unpublished_score_set['urn']}/publish")
    assert publish_score_set_response.status_code == 200
    published_score_set = publish_score_set_response.json()
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published_score_set["urn"]
    superseding_score_set_response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert superseding_score_set_response.status_code == 200
    superseding_score_set = superseding_score_set_response.json()
    change_ownership(session, superseding_score_set["urn"], ScoreSetDbModel)
    score_set_response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}")
    score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    assert score_set["urn"] == superseding_score_set["supersededScoreSet"]["urn"]
    # Other users can't view the unpublished superseding score set.
    assert "supersedingScoreSet" not in score_set

def test_can_view_others_published_superseding_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    publish_score_set_response = client.post(f"/api/v1/score-sets/{unpublished_score_set['urn']}/publish")
    assert publish_score_set_response.status_code == 200
    published_score_set = publish_score_set_response.json()

    superseding_score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        published_score_set["experiment"]["urn"],
        data_files / "scores.csv",
        update={"supersededScoreSetUrn": published_score_set["urn"]},
    )
    published_superseding_score_set_response = client.post(f"/api/v1/score-sets/{superseding_score_set['urn']}/publish")
    assert published_superseding_score_set_response.status_code == 200
    published_superseding_score_set = published_superseding_score_set_response.json()

    change_ownership(session, published_superseding_score_set["urn"], ScoreSetDbModel)

    score_set_response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}")
    assert score_set_response.status_code == 200
    score_set = score_set_response.json()
    assert score_set["urn"] == published_superseding_score_set["supersededScoreSet"]["urn"]
    # Other users can view published superseding score set.
    assert score_set["supersedingScoreSet"]["urn"] == published_superseding_score_set["urn"]


# The superseding score set is unpublished so the newest version to its owner is the unpublished one.
def test_show_correct_score_set_version_with_superseded_score_set_to_its_owner(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    publish_score_set_response = client.post(f"/api/v1/score-sets/{unpublished_score_set['urn']}/publish")
    assert publish_score_set_response.status_code == 200
    published_score_set = publish_score_set_response.json()
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published_score_set["urn"]
    superseding_score_set_response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert superseding_score_set_response.status_code == 200
    superseding_score_set = superseding_score_set_response.json()
    score_set_response = client.get(f"/api/v1/score-sets/{superseding_score_set['urn']}")
    score_set = score_set_response.json()
    assert score_set_response.status_code == 200
    assert score_set["urn"] == superseding_score_set["urn"]