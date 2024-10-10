import re
from copy import deepcopy
from datetime import date
from unittest.mock import patch

import jsonschema
import pytest
import requests
import requests_mock

from mavedb.lib.validation.urn_re import MAVEDB_TMP_URN_RE
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.experiment_set import ExperimentSet as ExperimentSetDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.orcid import OrcidUser
from tests.helpers.util import (
    add_contributor,
    change_ownership,
    create_experiment,
    create_seq_score_set,
    create_seq_score_set_with_variants,
)
from tests.helpers.constants import (
    EXTRA_USER,
    TEST_BIORXIV_IDENTIFIER,
    TEST_CROSSREF_IDENTIFIER,
    TEST_EXPERIMENT_WITH_KEYWORD,
    TEST_EXPERIMENT_WITH_KEYWORD_RESPONSE,
    TEST_EXPERIMENT_WITH_KEYWORD_HAS_DUPLICATE_OTHERS_RESPONSE,
    TEST_MEDRXIV_IDENTIFIER,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_EXPERIMENT_RESPONSE,
    TEST_ORCID_ID,
    TEST_PUBMED_IDENTIFIER,
    TEST_USER,
)
from tests.helpers.dependency_overrider import DependencyOverrider


def test_test_minimal_experiment_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_EXPERIMENT, schema=ExperimentCreate.schema())


def test_create_minimal_experiment(client, setup_router_db):
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "experimentSetUrn": response_data["experimentSetUrn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_create_experiment_with_contributor(client, setup_router_db):
    experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment.update({"contributors": [{"orcid_id": TEST_ORCID_ID}]})

    with patch(
        "mavedb.lib.orcid.fetch_orcid_user",
        lambda orcid_id: OrcidUser(orcid_id=orcid_id, given_name="ORCID", family_name="User"),
    ):
        response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "experimentSetUrn": response_data["experimentSetUrn"]})
    expected_response["contributors"] = [
        {
            "orcidId": TEST_ORCID_ID,
            "givenName": "ORCID",
            "familyName": "User",
        }
    ]
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_create_experiment_with_keywords(session, client, setup_router_db):
    response = client.post("/api/v1/experiments/", json=TEST_EXPERIMENT_WITH_KEYWORD)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_EXPERIMENT_WITH_KEYWORD_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "experimentSetUrn": response_data["experimentSetUrn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_cannot_create_experiment_without_email(client, setup_router_db):
    client.put("api/v1/users/me", json={"email": None})
    response = client.post("/api/v1/experiments/", json=TEST_MINIMAL_EXPERIMENT)
    assert response.status_code == 400
    response_data = response.json()
    assert response_data["detail"] == "There must be an email address associated with your account to use this feature."


def test_cannot_create_experiment_that_keyword_does_not_match_db_keyword(client, setup_router_db):
    # Database does not have this keyword.
    invalid_keyword = {
        "keywords": [
            {
                "keyword": {
                    "key": "Invalid key",
                    "value": "Invalid value",
                },
            }
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **invalid_keyword}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"] == "Invalid keyword Invalid key or Invalid value"


def test_cannot_create_experiment_that_keywords_has_wrong_combination1(client, setup_router_db):
    # Test src/mavedb/lib/validation/keywords.validate_keyword_keys function
    wrong_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Endogenous locus library method",
                    "special": False,
                    "description": "Description",
                },
            },
            {
                "keyword": {
                    "key": "In Vitro Construct Library Method System",
                    "value": "Oligo-directed mutagenic PCR",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **wrong_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"]
        == "If 'Variant Library Creation Method' is 'Endogenous locus library method', both 'Endogenous Locus "
        "Library Method System' and 'Endogenous Locus Library Method Mechanism' must be present."
    )


def test_cannot_create_experiment_that_keywords_has_wrong_combination2(client, setup_router_db):
    # Test src/mavedb/lib/validation/keywords.validate_keyword_keys function
    wrong_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "In vitro construct library method",
                    "special": False,
                    "description": "Description",
                },
            },
            {
                "keyword": {
                    "key": "Endogenous Locus Library Method System",
                    "value": "SaCas9",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **wrong_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"]
        == "If 'Variant Library Creation Method' is 'In vitro construct library method', both 'In Vitro Construct "
        "Library Method System' and 'In Vitro Construct Library Method Mechanism' must be present."
    )


def test_cannot_create_experiment_that_keywords_has_wrong_combination3(client, setup_router_db):
    """
    Test src/mavedb/lib/validation/keywords.validate_keyword_keys function
    If choose Other in Variant Library Creation Method, should not have Endogenous
    """
    wrong_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Other",
                    "special": False,
                    "description": "Description",
                },
                "description": "Description",
            },
            {
                "keyword": {
                    "key": "Endogenous Locus Library Method System",
                    "value": "SaCas9",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **wrong_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"]
        == "If 'Variant Library Creation Method' is 'Other', none of 'Endogenous Locus Library Method System', "
        "'Endogenous Locus Library Method Mechanism', 'In Vitro Construct Library Method System', or 'In Vitro "
        "Construct Library Method Mechanism' should be present."
    )


def test_cannot_create_experiment_that_keywords_has_wrong_combination3(client, setup_router_db):
    """
    Test src/mavedb/lib/validation/keywords.validate_keyword_keys function
    If choose Other in Variant Library Creation Method, should not have in vitro
    """
    wrong_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Other",
                    "special": False,
                    "description": "Description",
                },
                "description": "Description",
            },
            {
                "keyword": {
                    "key": "In Vitro Construct Library Method System",
                    "value": "Error-prone PCR",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **wrong_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        response_data["detail"]
        == "If 'Variant Library Creation Method' is 'Other', none of 'Endogenous Locus Library Method System', "
        "'Endogenous Locus Library Method Mechanism', 'In Vitro Construct Library Method System', or 'In Vitro "
        "Construct Library Method Mechanism' should be present."
    )


def test_cannot_create_experiment_that_keyword_value_is_other_without_description(client, setup_router_db):
    """
    Test src/mavedb/lib/validation/keywords.validate_description function
    If choose other, description should not be null.
    """
    invalid_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Other",
                    "special": False,
                    "description": "Description",
                },
                "description": None,
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **invalid_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    error_messages = [error["msg"] for error in response_data["detail"]]
    assert "Other option does not allow empty description." in error_messages


def test_cannot_create_experiment_that_keywords_have_duplicate_keys(client, setup_router_db):
    # Test src/mavedb/lib/validation/keywords.validate_duplicates function
    invalid_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Other",
                    "special": False,
                    "description": "Description",
                },
                "description": "Description",
            },
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "In vitro construct library method",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **invalid_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"] == "Duplicate keys found in keywords."


def test_cannot_create_experiment_that_keywords_have_duplicate_values(client, setup_router_db):
    """
    Test src/mavedb/lib/validation/keywords.validate_duplicates function
    Keyword values are not allowed duplicates except Other.
    """
    invalid_keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Delivery method",
                    "value": "In vitro construct library method",
                    "special": False,
                    "description": "Description",
                },
            },
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "In vitro construct library method",
                    "special": False,
                    "description": "Description",
                },
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **invalid_keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 422
    response_data = response.json()
    assert response_data["detail"] == "Duplicate values found in keywords."


def test_create_experiment_that_keywords_have_duplicate_others(client, setup_router_db):
    """
    Test src/mavedb/lib/validation/keywords.validate_duplicates function
    Keyword values are not allowed duplicates except Other.
    """
    keywords = {
        "keywords": [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Other",
                    "special": False,
                    "description": "Description",
                },
                "description": "Description",
            },
            {
                "keyword": {"key": "Delivery method", "value": "Other", "special": False, "description": "Description"},
                "description": "Description",
            },
        ]
    }
    experiment = {**TEST_MINIMAL_EXPERIMENT, **keywords}
    response = client.post("/api/v1/experiments/", json=experiment)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["experimentSetUrn"]), re.Match)
    expected_response = deepcopy(TEST_EXPERIMENT_WITH_KEYWORD_HAS_DUPLICATE_OTHERS_RESPONSE)
    expected_response.update({"urn": response_data["urn"], "experimentSetUrn": response_data["experimentSetUrn"]})
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_can_delete_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    response = client.delete(f"api/v1/experiments/{experiment['urn']}")
    assert response.status_code == 200
    get_response = client.get(f"api/v1/experiments/{experiment['urn']}")
    assert get_response.status_code == 404


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("urn", "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"),
        ("numScoreSets", 2),
        ("createdBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("modifiedBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("creationDate", date(2020, 4, 1).isoformat()),
        ("modificationDate", date(2020, 4, 1).isoformat()),
        ("publishedDate", date(2020, 4, 1).isoformat()),
    ],
)
def test_cannot_create_special_fields(client, setup_router_db, test_field, test_value):
    response_data = create_experiment(client, {test_field: test_value})
    if test_field in response_data:
        assert (test_field, response_data[test_field]) != (test_field, test_value)


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("createdBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("modifiedBy", {"firstName": "Sneaky", "lastName": "User", "orcidId": "0000-9999-9999-0000"}),
        ("creationDate", date(2020, 4, 1).isoformat()),
        ("modificationDate", date(2020, 4, 1).isoformat()),
        ("publishedDate", date(2020, 4, 1).isoformat()),
    ],
)
def test_cannot_edit_special_fields(client, setup_router_db, test_field, test_value):
    experiment = create_experiment(client)
    response_data = create_experiment(client, {test_field: test_value, "urn": experiment["urn"]})
    if test_field in response_data:
        assert (test_field, response_data[test_field]) != (test_field, test_value)


def test_cannot_assign_to_missing_experiment_set(client, setup_router_db):
    experiment_set_urn = "tmp:33df10c9-78b3-4e04-bafb-2446078573d7"
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment_set_urn})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment set with URN '{experiment_set_urn}' not found" in response_data["detail"]


def test_can_update_own_private_experiment_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment["experimentSetUrn"], "title": "Second Experiment"})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 200
    assert response.json()["experimentSetUrn"] == experiment["experimentSetUrn"]
    assert response.json()["title"] == "Second Experiment"


def test_cannot_update_other_users_private_experiment_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment set with URN '{experiment['experimentSetUrn']}' not found" in response_data["detail"]


def test_anonymous_cannot_update_other_users_private_experiment_set(
    session, client, anonymous_app_overrides, setup_router_db
):
    experiment = create_experiment(client)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment["experimentSetUrn"]})

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post("/api/v1/experiments/", json=experiment_post_payload)

    assert response.status_code == 401
    response_data = response.json()
    assert "Could not validate credentials" in response_data["detail"]


def test_admin_can_update_other_users_private_experiment_set(session, client, admin_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": experiment["experimentSetUrn"], "title": "Second Experiment"})

    with DependencyOverrider(admin_app_overrides):
        response = client.post("/api/v1/experiments/", json=experiment_post_payload)

    assert response.status_code == 200
    assert response.json()["experimentSetUrn"] == experiment["experimentSetUrn"]
    assert response.json()["title"] == "Second Experiment"


def test_can_update_own_public_experiment_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    response_data = create_experiment(
        client,
        {"experimentSetUrn": published_score_set["experiment"]["experimentSetUrn"], "title": "Second Experiment"},
    )
    assert response_data["experimentSetUrn"] == published_score_set["experiment"]["experimentSetUrn"]
    assert response_data["title"] == "Second Experiment"


def test_cannot_update_other_users_public_experiment_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    published_experiment_set_urn = published_score_set["experiment"]["experimentSetUrn"]
    change_ownership(session, published_experiment_set_urn, ExperimentSetDbModel)
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": published_experiment_set_urn, "title": "Second Experiment"})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions for URN '{published_experiment_set_urn}'" in response_data["detail"]


def test_anonymous_cannot_update_others_user_public_experiment_set(
    session, data_provider, client, anonymous_app_overrides, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    published_experiment_set_urn = published_score_set["experiment"]["experimentSetUrn"]
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"experimentSetUrn": published_experiment_set_urn, "title": "Second Experiment"})

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post("/api/v1/experiments/", json=experiment_post_payload)

    assert response.status_code == 401
    response_data = response.json()
    assert f"Could not validate credentials" in response_data["detail"]


def test_admin_can_update_other_users_public_experiment_set(
    session, data_provider, client, admin_app_overrides, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()

    with DependencyOverrider(admin_app_overrides):
        response_data = create_experiment(
            client,
            {"experimentSetUrn": published_score_set["experiment"]["experimentSetUrn"], "title": "Second Experiment"},
        )
    assert response_data["experimentSetUrn"] == published_score_set["experiment"]["experimentSetUrn"]
    assert response_data["title"] == "Second Experiment"


def test_edit_preserves_optional_metadata(client, setup_router_db):
    pass


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_can_edit_private_experiment(client, setup_router_db, test_field, test_value):
    experiment = create_experiment(client)
    experiment_post_payload = experiment.copy()
    experiment_post_payload.update({test_field: test_value, "urn": experiment["urn"]})
    response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert (test_field, response_data[test_field]) == (test_field, test_value)


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_cannot_edit_other_users_private_experiment(client, session, setup_router_db, test_field, test_value):
    experiment = create_experiment(client)
    experiment_post_payload = experiment.copy()
    experiment_post_payload.update({test_field: test_value, "urn": experiment["urn"]})
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=experiment_post_payload)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment['urn']}' not found" in response_data["detail"]


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_anonymous_cannot_update_other_users_private_experiment(
    client, anonymous_app_overrides, session, setup_router_db, test_field, test_value
):
    experiment = create_experiment(client)
    experiment_post_payload = experiment.copy()
    experiment_post_payload.update({test_field: test_value, "urn": experiment["urn"]})
    change_ownership(session, experiment["urn"], ExperimentDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=experiment_post_payload)

    assert response.status_code == 401
    response_data = response.json()
    assert f"Could not validate credentials" in response_data["detail"]


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_contributor_can_update_other_users_private_experiment(
    session, client, test_field, test_value, setup_router_db
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
    experiment_post_payload = experiment.copy()
    experiment_post_payload.update({test_field: test_value, "urn": experiment["urn"]})
    response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert (test_field, response_data[test_field]) == (test_field, test_value)


@pytest.mark.parametrize(
    "test_field,test_value",
    [
        ("title", "Edited Title"),
        ("shortDescription", "Edited Short Description"),
        ("abstractText", "Edited Abstract"),
        ("methodText", "Edited Methods"),
    ],
)
def test_admin_can_update_other_users_private_experiment(
    client, admin_app_overrides, setup_router_db, test_field, test_value
):
    experiment = create_experiment(client)
    experiment_post_payload = experiment.copy()
    experiment_post_payload.update({test_field: test_value, "urn": experiment["urn"]})
    with DependencyOverrider(admin_app_overrides):
        response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=experiment_post_payload)
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert (test_field, response_data[test_field]) == (test_field, test_value)


def test_can_edit_published_experiment(client, setup_router_db):
    pass


def test_cannot_edit_unowned_experiment(client, setup_router_db):
    pass


@pytest.mark.parametrize("test_field", ["title", "shortDescription", "abstractText", "methodText"])
def test_required_fields(client, setup_router_db, test_field):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    del experiment_post_payload[test_field]
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert "field required" in response_data["detail"][0]["msg"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_create_experiment_with_new_primary_pubmed_publication(client, setup_router_db, mock_publication_fetch):
    mocked_publication = mock_publication_fetch
    response_data = create_experiment(client, {"primaryPublicationIdentifiers": [mocked_publication]})

    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(
        [
            "abstract",
            "id",
            "authors",
            "dbName",
            "doi",
            "identifier",
            "title",
            "url",
            "referenceHtml",
            "publicationJournal",
            "publicationYear",
        ]
    )
    # TODO: add separate tests for generating the publication url and referenceHtml


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        ({"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"}),
        ({"dbName": "medRxiv", "identifier": f"{TEST_MEDRXIV_IDENTIFIER}"}),
    ],
    indirect=["mock_publication_fetch"],
)
def test_create_experiment_with_new_primary_preprint_publication(client, setup_router_db, mock_publication_fetch):
    mocked_publication = mock_publication_fetch
    response_data = create_experiment(client, {"primaryPublicationIdentifiers": [mocked_publication]})

    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(
        [
            "abstract",
            "id",
            "authors",
            "dbName",
            "identifier",
            "title",
            "url",
            "referenceHtml",
            "doi",
            "publicationJournal",
            "publicationYear",
        ]
    )
    # TODO: add separate tests for generating the publication url and referenceHtml


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        ({"dbName": "Crossref", "identifier": f"{TEST_CROSSREF_IDENTIFIER}"}),
    ],
    indirect=["mock_publication_fetch"],
)
def test_create_experiment_with_new_primary_crossref_publication(client, setup_router_db, mock_publication_fetch):
    mocked_publication = mock_publication_fetch
    response_data = create_experiment(client, {"primaryPublicationIdentifiers": [mocked_publication]})

    assert len(response_data["primaryPublicationIdentifiers"]) == 1
    assert sorted(response_data["primaryPublicationIdentifiers"][0]) == sorted(
        [
            "abstract",
            "id",
            "authors",
            "dbName",
            "identifier",
            "title",
            "url",
            "referenceHtml",
            "doi",
            "publicationJournal",
            "publicationYear",
        ]
    )
    # TODO: add separate tests for generating the publication url and referenceHtml


@pytest.mark.parametrize(
    "db_name, identifier", [("biorxiv", TEST_BIORXIV_IDENTIFIER), ("medrxiv", TEST_MEDRXIV_IDENTIFIER)]
)
def test_create_experiment_rxiv_not_found(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(
            f"https://api.biorxiv.org/details/{db_name}/10.1101/{identifier}/na/json",
            json={"messages": [{"status": "no posts found"}], "collection": []},
        )
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called

        assert r.status_code == 404


@pytest.mark.parametrize(
    "db_name, identifier", [("biorxiv", TEST_BIORXIV_IDENTIFIER), ("medrxiv", TEST_MEDRXIV_IDENTIFIER)]
)
def test_create_experiment_rxiv_timeout(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(
            f"https://api.biorxiv.org/details/{db_name}/10.1101/{identifier}/na/json",
            exc=requests.exceptions.ConnectTimeout,
        )
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called
        assert r.status_code == 504


@pytest.mark.parametrize(
    "db_name, identifier", [("biorxiv", TEST_BIORXIV_IDENTIFIER), ("medrxiv", TEST_MEDRXIV_IDENTIFIER)]
)
def test_create_experiment_rxiv_unavailable(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(f"https://api.biorxiv.org/details/{db_name}/10.1101/{identifier}/na/json", status_code=503)
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called
        assert r.status_code == 502


@pytest.mark.parametrize("db_name, identifier", [("Crossref", TEST_CROSSREF_IDENTIFIER)])
def test_create_experiment_crossref_not_found(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(f"https://api.crossref.org/works/{identifier}", json="Resource not found.", status_code=404)
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called

        assert r.status_code == 404


@pytest.mark.parametrize("db_name, identifier", [("Crossref", TEST_CROSSREF_IDENTIFIER)])
def test_create_experiment_crossref_timeout(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(
            f"https://api.crossref.org/works/{identifier}",
            exc=requests.exceptions.ConnectTimeout,
        )
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called
        assert r.status_code == 504


@pytest.mark.parametrize("db_name, identifier", [("Crossref", TEST_CROSSREF_IDENTIFIER)])
def test_create_experiment_crossref_unavailable(client, setup_router_db, db_name, identifier):
    with requests_mock.mock() as m:
        m.get(f"https://api.crossref.org/works/{identifier}", status_code=503)
        payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
        payload["primaryPublicationIdentifiers"] = [{"identifier": f"{identifier}"}]
        r = client.post("/api/v1/experiments/", json=payload)

        assert m.called
        assert r.status_code == 502


def test_create_experiment_with_invalid_doi(client, setup_router_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"doiIdentifiers": [{"identifier": "20711194"}]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['doiIdentifiers'][0]['identifier']}' is not a valid DOI identifier"
        in response_data["detail"][0]["msg"]
    )


def test_create_experiment_with_invalid_primary_publication(client, setup_router_db):
    experiment_post_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    experiment_post_payload.update({"primaryPublicationIdentifiers": [{"identifier": "abcdefg"}]})
    response = client.post("/api/v1/experiments/", json=experiment_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert (
        f"'{experiment_post_payload['primaryPublicationIdentifiers'][0]['identifier']}' is not a valid DOI or a valid PubMed, bioRxiv, or medRxiv identifier"
        in response_data["detail"][0]["msg"]
    )


def test_get_own_private_experiment(client, setup_router_db):
    experiment = create_experiment(client)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": experiment["urn"], "experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.get(f"/api/v1/experiments/{experiment['urn']}")
    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_cannot_get_other_users_private_experiment(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    response = client.get(f"/api/v1/experiments/{experiment['urn']}")
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment['urn']}' not found" in response_data["detail"]


def test_anonymous_cannot_get_users_private_experiment(session, client, anonymous_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment with URN '{experiment['urn']}' not found" in response_data["detail"]


def test_admin_can_get_other_users_private_experiment(client, admin_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    expected_response = deepcopy(TEST_MINIMAL_EXPERIMENT_RESPONSE)
    expected_response.update({"urn": experiment["urn"], "experimentSetUrn": experiment["experimentSetUrn"]})
    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=Experiment.schema())
    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])


def test_search_experiments(session, client, setup_router_db):
    experiment = create_experiment(client)
    search_payload = {"text": experiment["shortDescription"]}
    response = client.post("/api/v1/experiments/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()[0]["title"] == experiment["title"]


def test_search_my_experiments(session, client, setup_router_db):
    experiment = create_experiment(client)
    search_payload = {"text": experiment["shortDescription"]}
    response = client.post("/api/v1/me/experiments/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()[0]["title"] == experiment["title"]


def test_search_score_sets_for_experiments(session, client, setup_router_db, data_files, data_provider):
    experiment = create_experiment(client)
    score_set_pub = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    # make the unpublished score set owned by some other user. This shouldn't appear in the results.
    score_set_unpub = create_seq_score_set(client, experiment["urn"], update={"title": "Unpublished Score Set"})
    published_score_set = client.post(f"/api/v1/score-sets/{score_set_pub['urn']}/publish").json()
    change_ownership(session, score_set_unpub["urn"], ScoreSetDbModel)

    # On score set publication, the experiment will get a new urn
    experiment_urn = published_score_set["experiment"]["urn"]
    response = client.get(f"/api/v1/experiments/{experiment_urn}/score-sets")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["urn"] == published_score_set["urn"]


def test_search_score_sets_for_contributor_experiments(session, client, setup_router_db, data_files, data_provider):
    experiment = create_experiment(client)
    score_set_pub = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    # make the unpublished score set owned by some other user. This shouldn't appear in the results.
    score_set_unpub = create_seq_score_set(client, experiment["urn"], update={"title": "Unpublished Score Set"})
    published_score_set = client.post(f"/api/v1/score-sets/{score_set_pub['urn']}/publish").json()
    change_ownership(session, score_set_unpub["urn"], ScoreSetDbModel)
    add_contributor(
        session,
        score_set_unpub["urn"],
        ScoreSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )

    # On score set publication, the experiment will get a new urn
    experiment_urn = published_score_set["experiment"]["urn"]
    response = client.get(f"/api/v1/experiments/{experiment_urn}/score-sets")
    assert response.status_code == 200
    response_urns = [score_set["urn"] for score_set in response.json()]
    assert len(response_urns) == 2
    assert published_score_set["urn"] in response_urns
    assert score_set_unpub["urn"] in response_urns


def test_search_score_sets_for_my_experiments(session, client, setup_router_db, data_files, data_provider):
    experiment = create_experiment(client)
    score_set_pub = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    # The unpublished score set is for the current user, so it should show up in results.
    score_set_unpub = create_seq_score_set(client, experiment["urn"], update={"title": "Unpublished Score Set"})
    published_score_set = client.post(f"/api/v1/score-sets/{score_set_pub['urn']}/publish").json()

    # On score set publication, the experiment will get a new urn
    experiment_urn = published_score_set["experiment"]["urn"]
    response = client.get(f"/api/v1/experiments/{experiment_urn}/score-sets")
    assert response.status_code == 200
    assert len(response.json()) == 2
    assert set(score_set["urn"] for score_set in response.json()) == set(
        (published_score_set["urn"], score_set_unpub["urn"])
    )


def test_search_their_experiments(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    search_payload = {"text": experiment["shortDescription"]}
    response = client.post("/api/v1/experiments/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()[0]["createdBy"]["orcidId"] == EXTRA_USER["username"]
    assert response.json()[0]["createdBy"]["firstName"] == EXTRA_USER["first_name"]


def test_search_not_my_experiments(session, client, setup_router_db):
    experiment = create_experiment(client)
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment["experimentSetUrn"], ExperimentSetDbModel)
    search_payload = {"text": experiment["shortDescription"]}
    response = client.post("/api/v1/me/experiments/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_anonymous_search_experiments(session, client, anonymous_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    search_payload = {"text": experiment["shortDescription"]}
    with DependencyOverrider(anonymous_app_overrides):
        response = client.post("/api/v1/experiments/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()[0]["title"] == experiment["title"]


def test_anonymous_cannot_search_my_experiments(session, client, anonymous_app_overrides, setup_router_db):
    experiment = create_experiment(client)
    search_payload = {"text": experiment["shortDescription"]}
    with DependencyOverrider(anonymous_app_overrides):
        response = client.post("/api/v1/me/experiments/search", json=search_payload)
    assert response.status_code == 401
    assert response.json()["detail"] in "Could not validate credentials"


def test_anonymous_cannot_delete_other_users_private_experiment(
    session, client, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    with DependencyOverrider(anonymous_app_overrides):
        response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 401
    assert "Could not validate credentials" in response.json()["detail"]


def test_anonymous_cannot_delete_other_users_published_experiment(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")

    with DependencyOverrider(anonymous_app_overrides):
        del_response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert del_response.status_code == 401
    del_response_data = del_response.json()
    assert "Could not validate credentials" in del_response_data["detail"]


def test_can_delete_own_private_experiment(session, client, setup_router_db):
    experiment = create_experiment(client)
    response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 200


def test_cannot_delete_own_published_experiment(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
    response_data = response.json()
    experiment_urn = response_data["experiment"]["urn"]
    del_response = client.delete(f"/api/v1/experiments/{experiment_urn}")

    assert del_response.status_code == 403
    del_response_data = del_response.json()
    assert f"insufficient permissions for URN '{experiment_urn}'" in del_response_data["detail"]


def test_contributor_can_delete_other_users_private_experiment(session, client, setup_router_db, admin_app_overrides):
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
    response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 200


def test_admin_can_delete_other_users_private_experiment(session, client, setup_router_db, admin_app_overrides):
    experiment = create_experiment(client)
    with DependencyOverrider(admin_app_overrides):
        response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert response.status_code == 200


def test_contributor_can_delete_other_users_published_experiment(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    client.post(f"/api/v1/experiments/{score_set['urn']}/publish")
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    add_contributor(
        session,
        experiment["urn"],
        ExperimentDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    del_response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert del_response.status_code == 200


def test_admin_can_delete_other_users_published_experiment(
    session, data_provider, client, setup_router_db, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    client.post(f"/api/v1/experiments/{score_set['urn']}/publish")
    with DependencyOverrider(admin_app_overrides):
        del_response = client.delete(f"/api/v1/experiments/{experiment['urn']}")

    assert del_response.status_code == 200


def test_can_add_experiment_to_own_private_experiment_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 200


def test_can_add_experiment_to_own_public_experiment_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": published_score_set["experiment"]["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 200


def test_contributor_can_add_experiment_to_others_private_experiment_set(session, client, setup_router_db):
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
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": experiment["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 200


def test_contributor_can_add_experiment_to_others_public_experiment_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    change_ownership(session, published_score_set["experiment"]["urn"], ExperimentDbModel)
    change_ownership(session, published_score_set["experiment"]["experimentSetUrn"], ExperimentSetDbModel)
    add_contributor(
        session,
        published_score_set["experiment"]["experimentSetUrn"],
        ExperimentSetDbModel,
        TEST_USER["username"],
        TEST_USER["first_name"],
        TEST_USER["last_name"],
    )
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": published_score_set["experiment"]["experimentSetUrn"]})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 200


def test_cannot_add_experiment_to_others_private_experiment_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    experiment_set_urn = experiment["experimentSetUrn"]
    change_ownership(session, experiment["urn"], ExperimentDbModel)
    change_ownership(session, experiment_set_urn, ExperimentSetDbModel)
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": experiment_set_urn})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 404
    response_data = response.json()
    assert f"experiment set with URN '{experiment_set_urn}' not found" in response_data["detail"]


def test_cannot_add_experiment_to_others_public_experiment_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    published_score_set = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish").json()
    experiment_set_urn = published_score_set["experiment"]["experimentSetUrn"]
    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    change_ownership(session, published_score_set["experiment"]["urn"], ExperimentDbModel)
    change_ownership(session, experiment_set_urn, ExperimentSetDbModel)
    test_experiment = deepcopy(TEST_MINIMAL_EXPERIMENT)
    test_experiment.update({"experimentSetUrn": experiment_set_urn})
    response = client.post("/api/v1/experiments/", json=test_experiment)
    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions for URN '{experiment_set_urn}'" in response_data["detail"]
