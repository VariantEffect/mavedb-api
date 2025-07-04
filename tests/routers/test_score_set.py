# ruff: noqa: E402

import re
from copy import deepcopy
import csv
from datetime import date
from io import StringIO
from unittest.mock import patch

import jsonschema
import pytest
from humps import camelize
from sqlalchemy import select, delete

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE, MAVEDB_SCORE_SET_URN_RE, MAVEDB_EXPERIMENT_URN_RE
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.view_models.orcid import OrcidUser
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate

from tests.helpers.constants import (
    EXTRA_USER,
    EXTRA_LICENSE,
    TEST_CROSSREF_IDENTIFIER,
    TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION,
    TEST_MAPPED_VARIANT_WITH_HGVS_P_EXPRESSION,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET_RESPONSE,
    TEST_PUBMED_IDENTIFIER,
    TEST_ORCID_ID,
    TEST_SAVED_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE,
    TEST_SCORE_SET_RANGE,
    TEST_SAVED_SCORE_SET_RANGE,
    TEST_MINIMAL_ACC_SCORESET_RESPONSE,
    TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE,
    TEST_USER,
    TEST_INACTIVE_LICENSE,
    SAVED_DOI_IDENTIFIER,
    SAVED_EXTRA_CONTRIBUTOR,
    SAVED_PUBMED_PUBLICATION,
    SAVED_SHORT_EXTRA_LICENSE,
    TEST_SCORE_CALIBRATION,
    TEST_SAVED_SCORE_CALIBRATION,
    TEST_SAVED_CLINVAR_CONTROL,
    TEST_SAVED_GENERIC_CLINICAL_CONTROL,
    TEST_SCORE_SET_RANGE_WITH_ODDS_PATH,
    TEST_SAVED_SCORE_SET_RANGE_WITH_ODDS_PATH,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.common import update_expected_response_for_created_resources
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.license import change_to_inactive_license
from tests.helpers.util.score_set import (
    create_seq_score_set,
    create_seq_score_set_with_mapped_variants,
    link_clinical_controls_to_mapped_variants,
    publish_score_set,
)
from tests.helpers.util.user import change_ownership
from tests.helpers.util.variant import create_mapped_variants_for_score_set, mock_worker_variant_insertion


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
    expected_response["experiment"].update({"numScoreSets": 1})

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
    expected_response["experiment"].update({"numScoreSets": 1})

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


@pytest.mark.parametrize(
    "score_ranges,saved_score_ranges",
    [
        (TEST_SCORE_SET_RANGE, TEST_SAVED_SCORE_SET_RANGE),
        (TEST_SCORE_SET_RANGE_WITH_ODDS_PATH, TEST_SAVED_SCORE_SET_RANGE_WITH_ODDS_PATH),
    ],
)
def test_create_score_set_with_score_range(client, setup_router_db, score_ranges, saved_score_ranges):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"score_ranges": score_ranges})

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response["experiment"].update({"numScoreSets": 1})
    expected_response["scoreRanges"] = saved_score_ranges

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


@pytest.mark.parametrize("publication_list", ["primary_publication_identifiers", "secondary_publication_identifiers"])
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_create_score_set_with_score_range_and_odds_path_source(
    client, setup_router_db, publication_list, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set[publication_list] = TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE["odds_path_source"]
    score_set.update({"score_ranges": TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE})

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response[camelize(publication_list)] = [SAVED_PUBMED_PUBLICATION]
    expected_response["experiment"].update({"numScoreSets": 1})
    expected_response["scoreRanges"] = TEST_SAVED_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


def test_cannot_create_score_set_with_score_range_and_odds_path_source_when_publication_not_in_publications(
    client, setup_router_db
):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"score_ranges": TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE})

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 422

    response_data = response.json()
    assert (
        "Odds path source publication identifier at index 0 is not defined in score set publications."
        in response_data["detail"][0]["msg"]
    )


def test_remove_score_range_from_score_set(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"score_ranges": TEST_SCORE_SET_RANGE})

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response["experiment"].update({"numScoreSets": 1})
    expected_response["scoreRanges"] = TEST_SAVED_SCORE_SET_RANGE

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    score_set.pop("score_ranges")
    response = client.put(f"/api/v1/score-sets/{response_data['urn']}", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    assert "scoreRanges" not in response_data.keys()


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
        ("score_ranges", TEST_SCORE_SET_RANGE, TEST_SAVED_SCORE_SET_RANGE),
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
    expected_response["experiment"].update({"numScoreSets": 1})

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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    published_urn = published_score_set["urn"]
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
    expected_response["experiment"].update({"numScoreSets": 1})

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
            TEST_SCORE_SET_RANGE,
            None,
        ),
    ],
)
def test_cannot_update_score_set_target_data_after_publication(
    client, setup_router_db, attribute, expected_response_data, updated_data, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    published_urn = published_score_set["urn"]
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
    expected_response["experiment"].update({"numScoreSets": 1})

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
    expected_response["experiment"].update({"numScoreSets": 1})

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


def test_can_add_contributor_in_both_experiment_and_score_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
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
    score_set_response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert score_set_response.status_code == 200
    ss_response_data = score_set_response.json()
    assert len(ss_response_data["contributors"]) == 1
    assert any(c["orcidId"] == TEST_USER["username"] for c in ss_response_data["contributors"])
    experiment_response = client.get(f"/api/v1/experiments/{experiment['urn']}")
    assert experiment_response.status_code == 200
    exp_response_data = experiment_response.json()
    assert len(exp_response_data["contributors"]) == 1
    assert any(c["orcidId"] == TEST_USER["username"] for c in exp_response_data["contributors"])


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
    expected_response["experiment"].update({"numScoreSets": 1})

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
    expected_response["experiment"].update({"numScoreSets": 1})
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_score_set["urn"]), re.Match)
    assert isinstance(MAVEDB_EXPERIMENT_URN_RE.fullmatch(published_score_set["experiment"]["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), published_score_set["experiment"], published_score_set
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat(), "numScoreSets": 1})
    expected_response.update(
        {
            "urn": published_score_set["urn"],
            "publishedDate": date.today().isoformat(),
            "numVariants": 3,
            "private": False,
            "datasetColumns": {"countColumns": [], "scoreColumns": ["score"]},
            "processingState": ProcessingState.success.name,
        }
    )
    assert sorted(expected_response.keys()) == sorted(published_score_set.keys())

    # refresh score set to post worker state
    score_set = (client.get(f"/api/v1/score-sets/{published_score_set['urn']}")).json()
    for key in expected_response:
        assert (key, expected_response[key]) == (key, score_set[key])

    score_set_variants = session.execute(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])
    ).scalars()
    assert all([variant.urn.startswith("urn:mavedb:") for variant in score_set_variants])


def test_publish_multiple_score_sets(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"])
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"])
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")
    score_set_3 = create_seq_score_set(client, experiment["urn"])
    score_set_3 = mock_worker_variant_insertion(client, session, data_provider, score_set_3, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        pub_score_set_1_data = publish_score_set(client, score_set_1["urn"])
        pub_score_set_2_data = publish_score_set(client, score_set_2["urn"])
        pub_score_set_3_data = publish_score_set(client, score_set_3["urn"])
        worker_queue.assert_called()

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

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 422
        worker_queue.assert_not_called()
        response_data = response.json()

    assert "cannot publish score set without variant scores" in response_data["detail"]


def test_cannot_publish_other_user_private_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 404
        worker_queue.assert_not_called()
        response_data = response.json()

    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


def test_anonymous_cannot_publish_user_private_score_set(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with (
        DependencyOverrider(anonymous_app_overrides),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 401
        queue.assert_not_called()
        response_data = response.json()

    assert "Could not validate credentials" in response_data["detail"]


def test_contributor_can_publish_other_users_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    assert published_score_set["urn"] == "urn:mavedb:00000001-a-1"
    assert published_score_set["experiment"]["urn"] == "urn:mavedb:00000001-a"

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), published_score_set["experiment"], published_score_set
    )
    expected_response["experiment"].update({"publishedDate": date.today().isoformat(), "numScoreSets": 1})
    expected_response.update(
        {
            "urn": published_score_set["urn"],
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
    assert sorted(expected_response.keys()) == sorted(published_score_set.keys())

    # refresh score set to post worker state
    score_set = (client.get(f"/api/v1/score-sets/{published_score_set['urn']}")).json()
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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with (
        DependencyOverrider(admin_app_overrides),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
    ):
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 404
        queue.assert_not_called()
        response_data = response.json()

    assert f"score set with URN '{score_set['urn']}' not found" in response_data["detail"]


########################################################################################################################
# Score set meta-analysis
########################################################################################################################


def test_create_single_score_set_meta_analysis(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [published_score_set["urn"]]},
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    published_score_set_refresh = (client.get(f"/api/v1/score-sets/{published_score_set['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == [published_score_set_refresh["urn"]]
    assert published_score_set_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_publish_single_score_set_meta_analysis(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [score_set["urn"]]},
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)
    assert meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_single_experiment(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        published_score_set_2 = publish_score_set(client, score_set_2["urn"])
        worker_queue.assert_called()

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"], published_score_set_2["urn"]],
        },
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    published_score_set_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1["urn"], published_score_set_2["urn"]]
    )
    assert published_score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set["urn"]), re.Match)
    assert published_meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1 = create_seq_score_set(client, experiment_1["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment_2["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        published_score_set_2 = publish_score_set(client, score_set_2["urn"])
        worker_queue.assert_called()

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"], published_score_set_2["urn"]],
        },
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )
    published_score_set_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1["urn"], published_score_set_2["urn"]]
    )
    assert published_score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set["urn"]), re.Match)
    assert published_meta_score_set["urn"] == "urn:mavedb:00000003-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiments(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(
        client, {"title": "Experiment 2", "experimentSetUrn": experiment_1["experimentSetUrn"]}
    )
    score_set_1 = create_seq_score_set(client, experiment_1["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment_2["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        published_score_set_2 = publish_score_set(client, score_set_2["urn"])
        worker_queue.assert_called()

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"], published_score_set_2["urn"]],
        },
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )
    published_score_set_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1["urn"], published_score_set_2["urn"]]
    )
    assert published_score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set["urn"]), re.Match)
    assert published_meta_score_set["urn"] == "urn:mavedb:00000001-0-1"


def test_multiple_score_set_meta_analysis_multiple_experiment_sets_different_score_sets(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})

    score_set_1_1 = create_seq_score_set(client, experiment_1["urn"], update={"title": "Score Set 1 exp 1"})
    score_set_1_1 = mock_worker_variant_insertion(
        client, session, data_provider, score_set_1_1, data_files / "scores.csv"
    )
    score_set_2_1 = create_seq_score_set(client, experiment_1["urn"], update={"title": "Score Set 2 exp 1"})
    score_set_2_1 = mock_worker_variant_insertion(
        client, session, data_provider, score_set_2_1, data_files / "scores.csv"
    )
    score_set_1_2 = create_seq_score_set(client, experiment_2["urn"], update={"title": "Score Set 1 exp 2 "})
    score_set_1_2 = mock_worker_variant_insertion(
        client, session, data_provider, score_set_1_2, data_files / "scores.csv"
    )
    score_set_2_2 = create_seq_score_set(client, experiment_2["urn"], update={"title": "Score Set 2 exp 2"})
    score_set_2_2 = mock_worker_variant_insertion(
        client, session, data_provider, score_set_2_2, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1_1 = publish_score_set(client, score_set_1_1["urn"])
        published_score_set_1_2 = publish_score_set(client, score_set_1_2["urn"])
        published_score_set_2_1 = publish_score_set(client, score_set_2_1["urn"])
        published_score_set_2_2 = publish_score_set(client, score_set_2_2["urn"])
        worker_queue.assert_called()

    meta_score_set_1 = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1_1["urn"], published_score_set_1_2["urn"]],
        },
    )
    meta_score_set_1 = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set_1, data_files / "scores.csv"
    )

    published_score_set_1_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1_1['urn']}")).json()
    assert meta_score_set_1["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1_1["urn"], published_score_set_1_2["urn"]]
    )
    assert published_score_set_1_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set_1["urn"]]

    meta_score_set_2 = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_2_1["urn"], published_score_set_2_2["urn"]],
        },
    )
    meta_score_set_2 = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set_2, data_files / "scores.csv"
    )
    published_score_set_2_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_2_1['urn']}")).json()
    assert meta_score_set_2["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_2_1["urn"], published_score_set_2_2["urn"]]
    )
    assert published_score_set_2_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set_2["urn"]]

    meta_score_set_3 = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1_1["urn"], published_score_set_2_2["urn"]],
        },
    )
    meta_score_set_3 = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set_3, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_meta_score_set_1 = publish_score_set(client, meta_score_set_1["urn"])
        published_meta_score_set_2 = publish_score_set(client, meta_score_set_2["urn"])
        published_meta_score_set_3 = publish_score_set(client, meta_score_set_3["urn"])
        worker_queue.assert_called()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set_1["urn"]), re.Match)
    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set_2["urn"]), re.Match)
    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set_3["urn"]), re.Match)
    assert published_meta_score_set_1["urn"] == "urn:mavedb:00000003-0-1"
    assert published_meta_score_set_2["urn"] == "urn:mavedb:00000003-0-2"
    assert published_meta_score_set_3["urn"] == "urn:mavedb:00000003-0-3"


def test_cannot_add_score_set_to_meta_analysis_experiment(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        worker_queue.assert_called()

    meta_score_set_1 = create_seq_score_set(
        client,
        None,
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"]]},
    )
    meta_score_set_1 = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set_1, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        meta_score_set_1 = publish_score_set(client, meta_score_set_1["urn"])
        worker_queue.assert_called()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(meta_score_set_1["urn"]), re.Match)
    assert meta_score_set_1["urn"] == "urn:mavedb:00000001-0-1"

    score_set_2 = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_2["experimentUrn"] = meta_score_set_1["experiment"]["urn"]
    jsonschema.validate(instance=score_set_2, schema=ScoreSetCreate.schema())

    response = client.post("/api/v1/score-sets/", json=score_set_2)
    response_data = response.json()
    assert response.status_code == 403
    assert "Score sets may not be added to a meta-analysis experiment." in response_data["detail"]


def test_create_single_score_set_meta_analysis_to_others_score_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)

    meta_score_set = create_seq_score_set(
        client,
        None,
        update={"title": "Test Meta Analysis", "metaAnalyzesScoreSetUrns": [published_score_set["urn"]]},
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    published_score_set_refresh = (client.get(f"/api/v1/score-sets/{published_score_set['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == [published_score_set["urn"]]
    assert published_score_set_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_multiple_score_set_meta_analysis_single_experiment_with_different_creator(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        published_score_set_2 = publish_score_set(client, score_set_2["urn"])
        worker_queue.assert_called()

    change_ownership(session, published_score_set_2["urn"], ScoreSetDbModel)
    meta_score_set = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"], published_score_set_2["urn"]],
        },
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    published_score_set_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1["urn"], published_score_set_2["urn"]]
    )
    assert published_score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called()

    assert meta_score_set["urn"] == "urn:mavedb:00000001-0-1"
    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(meta_score_set["urn"]), re.Match)


def test_multiple_score_set_meta_analysis_multiple_experiment_sets_with_different_creator(
    session, data_provider, client, setup_router_db, data_files
):
    experiment_1 = create_experiment(client, {"title": "Experiment 1"})
    experiment_2 = create_experiment(client, {"title": "Experiment 2"})
    score_set_1 = create_seq_score_set(client, experiment_1["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment_2["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        published_score_set_2 = publish_score_set(client, score_set_2["urn"])
        worker_queue.assert_called()

    change_ownership(session, published_score_set_2["urn"], ScoreSetDbModel)
    meta_score_set = create_seq_score_set(
        client,
        None,
        update={
            "title": "Test Meta Analysis",
            "metaAnalyzesScoreSetUrns": [published_score_set_1["urn"], published_score_set_2["urn"]],
        },
    )
    meta_score_set = mock_worker_variant_insertion(
        client, session, data_provider, meta_score_set, data_files / "scores.csv"
    )

    published_score_set_1_refresh = (client.get(f"/api/v1/score-sets/{published_score_set_1['urn']}")).json()
    assert meta_score_set["metaAnalyzesScoreSetUrns"] == sorted(
        [published_score_set_1["urn"], published_score_set_2["urn"]]
    )
    assert published_score_set_1_refresh["metaAnalyzedByScoreSetUrns"] == [meta_score_set["urn"]]

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_meta_score_set = publish_score_set(client, meta_score_set["urn"])
        worker_queue.assert_called()

    assert published_meta_score_set["urn"] == "urn:mavedb:00000003-0-1"
    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_meta_score_set["urn"]), re.Match)


########################################################################################################################
# Score set search
########################################################################################################################


def test_search_private_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == score_set["title"]


def test_search_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"urn": score_set["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set["urn"]


# There is space in the end of test urn. The search result returned nothing before.
def test_search_private_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    urn_with_space = score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set["urn"]


def test_search_others_private_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"urn": score_set["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


# There is space in the end of test urn. The search result returned nothing before.
def test_search_others_private_score_sets_urn_with_space_match(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    urn_with_space = score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_public_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_public_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == score_set["title"]


def test_search_public_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_others_public_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_search_others_public_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    assert session.query(ScoreSetDbModel).filter_by(urn=published_score_set["urn"]).one()

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["title"] == published_score_set["title"]


def test_search_others_public_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    search_payload = {"urn": score_set["urn"]}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_others_public_score_sets_urn_with_space_match(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"urn": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_private_score_sets_not_showing_public_score_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set_1 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set_1["urn"])
        worker_queue.assert_called_once()

    search_payload = {"published": False}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == score_set_2["urn"]


def test_search_public_score_sets_not_showing_private_score_set(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set_1 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        worker_queue.assert_called_once()

    search_payload = {"published": True}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set_1["urn"]


########################################################################################################################
# Score set deletion
########################################################################################################################


def test_anonymous_cannot_delete_other_users_private_scoreset(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with DependencyOverrider(anonymous_app_overrides):
        response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]


def test_anonymous_cannot_delete_other_users_published_scoreset(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    with DependencyOverrider(anonymous_app_overrides):
        del_response = client.delete(f"/api/v1/score-sets/{published_score_set['urn']}")

    assert del_response.status_code == 401
    del_response_data = del_response.json()
    assert "Could not validate credentials" in del_response_data["detail"]


def test_can_delete_own_private_scoreset(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200


def test_cannot_delete_own_published_scoreset(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    del_response = client.delete(f"/api/v1/score-sets/{published_score_set['urn']}")

    assert del_response.status_code == 403
    del_response_data = del_response.json()
    assert f"insufficient permissions for URN '{published_score_set['urn']}'" in del_response_data["detail"]


def test_contributor_can_delete_other_users_private_scoreset(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with DependencyOverrider(admin_app_overrides):
        response = client.delete(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200


def test_admin_can_delete_other_users_published_scoreset(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    with DependencyOverrider(admin_app_overrides):
        del_response = client.delete(f"/api/v1/score-sets/{published_score_set['urn']}")
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
    score_set_1 = create_seq_score_set(client, experiment["urn"])
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set_1 = publish_score_set(client, score_set_1["urn"])
        worker_queue.assert_called_once()

    score_set_2 = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_2["experimentUrn"] = published_score_set_1["experiment"]["urn"]
    response = client.post("/api/v1/score-sets/", json=score_set_2)
    assert response.status_code == 200


def test_can_add_score_set_to_others_public_experiment(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"])
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set_1["urn"])
        worker_queue.assert_called_once()

    published_experiment_urn = published_score_set["experiment"]["urn"]
    change_ownership(session, published_experiment_urn, ExperimentDbModel)
    score_set_2 = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_2["experimentUrn"] = published_experiment_urn
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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    published_experiment_urn = published_score_set["experiment"]["urn"]
    change_ownership(session, published_experiment_urn, ExperimentDbModel)
    add_contributor(
        session,
        published_experiment_urn,
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_experiment_urn
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
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published_score_set["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published_score_set["urn"]
    superseding_score_set_response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert superseding_score_set_response.status_code == 200


def test_can_view_unpublished_superseding_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

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


def test_cannot_view_others_unpublished_superseding_score_set(
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
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    superseding_score_set = create_seq_score_set(
        client, published_score_set["experiment"]["urn"], update={"supersededScoreSetUrn": published_score_set["urn"]}
    )
    superseding_score_set = mock_worker_variant_insertion(
        client, session, data_provider, superseding_score_set, data_files / "scores.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_superseding_score_set = publish_score_set(client, superseding_score_set["urn"])
        worker_queue.assert_called_once()

    change_ownership(session, published_superseding_score_set["urn"], ScoreSetDbModel)

    score_set_response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}")
    assert score_set_response.status_code == 200
    score_set = score_set_response.json()
    assert score_set["urn"] == published_superseding_score_set["supersededScoreSet"]["urn"]
    # Other users can view published superseding score set.
    assert score_set["supersedingScoreSet"]["urn"] == published_superseding_score_set["urn"]


# The superseding score set is unpublished so the newest version to its owner is the unpublished one.
def test_show_correct_score_set_version_with_superseded_score_set_to_its_owner(
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


########################################################################################################################
# Score Calibrations
########################################################################################################################


def test_anonymous_user_cannot_add_score_calibrations_to_score_set(client, setup_router_db, anonymous_app_overrides):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    calibration_payload = deepcopy(TEST_SCORE_CALIBRATION)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/calibration/data", json={"test_calibrations": calibration_payload}
        )
        response_data = response.json()

    assert response.status_code == 401
    assert "score_calibrations" not in response_data


def test_user_cannot_add_score_calibrations_to_own_score_set(client, setup_router_db, anonymous_app_overrides):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    calibration_payload = deepcopy(TEST_SCORE_CALIBRATION)

    response = client.post(
        f"/api/v1/score-sets/{score_set['urn']}/calibration/data", json={"test_calibrations": calibration_payload}
    )
    response_data = response.json()

    assert response.status_code == 401
    assert "score_calibrations" not in response_data


def test_admin_can_add_score_calibrations_to_score_set(client, setup_router_db, admin_app_overrides):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    calibration_payload = deepcopy(TEST_SCORE_CALIBRATION)

    with DependencyOverrider(admin_app_overrides):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/calibration/data", json={"test_calibrations": calibration_payload}
        )
        response_data = response.json()

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )
    expected_response["scoreCalibrations"] = {"test_calibrations": deepcopy(TEST_SAVED_SCORE_CALIBRATION)}
    expected_response["experiment"].update({"numScoreSets": 1})

    assert response.status_code == 200
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_score_set_not_found_for_non_existent_score_set_when_adding_score_calibrations(
    client, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    calibration_payload = deepcopy(TEST_SCORE_CALIBRATION)

    with DependencyOverrider(admin_app_overrides):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']+'xxx'}/calibration/data",
            json={"test_calibrations": calibration_payload},
        )
        response_data = response.json()

    assert response.status_code == 404
    assert "score_calibrations" not in response_data


########################################################################################################################
# Score set upload files
########################################################################################################################


# Not sure why scores_non_utf8_encoded.csv file has a wrong encoding problem, but it's good for this test.
def test_upload_a_non_utf8_file(session, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores_non_utf8_encoded.csv"
    with open(scores_csv_path, "rb") as scores_file:
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
    assert response.status_code == 400
    response_data = response.json()
    assert (
        "Error decoding file: 'utf-8' codec can't decode byte 0xdd in position 10: invalid continuation byte. "
        "Ensure the file has correct values." in response_data["detail"]
    )


########################################################################################################################
# Score set download files
########################################################################################################################


@pytest.mark.parametrize(
    "mapped_variant,has_hgvs_g,has_hgvs_p",
    [
        (None, False, False),
        (TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION, True, False),
        (TEST_MAPPED_VARIANT_WITH_HGVS_P_EXPRESSION, False, True),
    ],
    ids=["without_post_mapped_vrs", "with_post_mapped_hgvs_g", "with_post_mapped_hgvs_p"],
)
def test_download_variants_data_file(
    session, data_provider, client, setup_router_db, data_files, mapped_variant, has_hgvs_g, has_hgvs_p
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    if mapped_variant is not None:
        create_mapped_variants_for_score_set(session, score_set["urn"], mapped_variant)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_scores_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?drop_na_columns=true"
    )
    assert download_scores_csv_response.status_code == 200
    download_scores_csv = download_scores_csv_response.text

    reader = csv.DictReader(StringIO(download_scores_csv))
    assert sorted(reader.fieldnames) == sorted(
        [
            "accession",
            "hgvs_nt",
            "hgvs_pro",
            "post_mapped_hgvs_g",
            "post_mapped_hgvs_p",
            "score",
        ]
    )
    rows = list(reader)
    for row in rows:
        if has_hgvs_g:
            assert row["post_mapped_hgvs_g"] == mapped_variant["post_mapped"]["expressions"][0]["value"]
        else:
            assert row["post_mapped_hgvs_g"] == "NA"
        if has_hgvs_p:
            assert row["post_mapped_hgvs_p"] == mapped_variant["post_mapped"]["expressions"][0]["value"]
        else:
            assert row["post_mapped_hgvs_p"] == "NA"


# Test file doesn't have hgvs_splice so its values are all NA.
def test_download_scores_file(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_scores_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/scores?drop_na_columns=true"
    )
    assert download_scores_csv_response.status_code == 200
    download_scores_csv = download_scores_csv_response.text
    csv_header = download_scores_csv.split("\n")[0]
    columns = csv_header.split(",")
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns


def test_download_counts_file(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_counts_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/counts?drop_na_columns=true"
    )
    assert download_counts_csv_response.status_code == 200
    download_counts_csv = download_counts_csv_response.text
    csv_header = download_counts_csv.split("\n")[0]
    columns = csv_header.split(",")
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns


########################################################################################################################
# Fetching clinical controls and control options for a score set
########################################################################################################################


def test_can_fetch_current_clinical_controls_for_score_set(client, setup_router_db, session, data_provider, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls")
    assert response.status_code == 200

    response_data = response.json()
    assert len(response_data) == 2
    for control in response_data:
        mapped_variants = control.pop("mappedVariants")
        assert len(mapped_variants) == 1
        assert all(
            control[k] in (TEST_SAVED_CLINVAR_CONTROL[k], TEST_SAVED_GENERIC_CLINICAL_CONTROL[k])
            for k in TEST_SAVED_CLINVAR_CONTROL.keys()
            if k != "mappedVariants"
        )


@pytest.mark.parametrize("clinical_control", [TEST_SAVED_CLINVAR_CONTROL, TEST_SAVED_GENERIC_CLINICAL_CONTROL])
@pytest.mark.parametrize(
    "parameters", [[("db", "dbName")], [("version", "dbVersion")], [("db", "dbName"), ("version", "dbVersion")]]
)
def test_can_fetch_current_clinical_controls_for_score_set_with_parameters(
    client, setup_router_db, session, data_provider, data_files, clinical_control, parameters
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    query_string = "?"
    for param, accessor in parameters:
        query_string += f"{param}={clinical_control[accessor]}&"

    # Remove the last '&' from the query string
    query_string = query_string.strip("&")

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls{query_string}")
    assert response.status_code == 200

    response_data = response.json()
    assert len(response_data)
    for param, accessor in parameters:
        assert all(control[accessor] == clinical_control[accessor] for control in response_data)


def test_cannot_fetch_clinical_controls_for_nonexistent_score_set(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']+'xxx'}/clinical-controls")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']+'xxx'}' not found" in response_data["detail"]


def test_cannot_fetch_clinical_controls_for_score_set_when_none_exist(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls")

    assert response.status_code == 404
    response_data = response.json()
    assert (
        f"No clinical control variants matching the provided filters associated with score set URN {score_set['urn']} were found"
        in response_data["detail"]
    )


def test_can_fetch_current_clinical_control_options_for_score_set(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls/options")
    assert response.status_code == 200

    response_data = response.json()
    assert len(response_data) == 2
    for control_option in response_data:
        assert len(control_option["availableVersions"]) == 1
        assert control_option["dbName"] in (
            TEST_SAVED_CLINVAR_CONTROL["dbName"],
            TEST_SAVED_GENERIC_CLINICAL_CONTROL["dbName"],
        )
        assert all(
            control_version
            in (TEST_SAVED_CLINVAR_CONTROL["dbVersion"], TEST_SAVED_GENERIC_CLINICAL_CONTROL["dbVersion"])
            for control_version in control_option["availableVersions"]
        )


def test_cannot_fetch_clinical_control_options_for_nonexistent_score_set(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']+'xxx'}/clinical-controls/options")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn']+'xxx'}' not found" in response_data["detail"]


def test_cannot_fetch_clinical_control_options_for_score_set_when_none_exist(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    # removes all clinical controls from the db.
    session.execute(delete(ClinicalControl))
    session.commit()

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls/options")
    print(response.json())

    assert response.status_code == 404
    response_data = response.json()
    assert (
        f"no clinical control variants associated with score set URN {score_set['urn']} were found"
        in response_data["detail"]
    )
