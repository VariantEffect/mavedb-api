# ruff: noqa: E402

import csv
import json
import re
from copy import deepcopy
from datetime import date
from io import StringIO
from unittest.mock import patch

import jsonschema
import pytest
from humps import camelize
from sqlalchemy import select

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.lib.annotation.annotate import variant_study_result
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.exceptions import NonexistentOrcidUserError
from mavedb.lib.validation.urn_re import MAVEDB_EXPERIMENT_URN_RE, MAVEDB_SCORE_SET_URN_RE, MAVEDB_TMP_URN_RE
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.experiment import Experiment as ExperimentDbModel
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_calibration import ScoreCalibration as ScoreCalibrationDbModel
from mavedb.models.mapped_variant import MappedVariant as MappedVariantDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.routers.score_sets import _annotation_stream_record
from mavedb.view_models.orcid import OrcidUser
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import (
    EXTRA_LICENSE,
    EXTRA_USER,
    SAVED_DOI_IDENTIFIER,
    SAVED_EXTRA_CONTRIBUTOR,
    SAVED_MINIMAL_DATASET_COLUMNS,
    SAVED_PUBMED_PUBLICATION,
    SAVED_SHORT_EXTRA_LICENSE,
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_CLINVAR_CONTROL,
    TEST_CROSSREF_IDENTIFIER,
    TEST_GNOMAD_DATA_VERSION,
    TEST_INACTIVE_LICENSE,
    TEST_KEYWORDS,
    TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION,
    TEST_MAPPED_VARIANT_WITH_HGVS_P_EXPRESSION,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_ACC_SCORESET_RESPONSE,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET_RESPONSE,
    TEST_ORCID_ID,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_PUBMED_IDENTIFIER,
    TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_SAVED_CLINVAR_CONTROL,
    TEST_SAVED_GENERIC_CLINICAL_CONTROL,
    TEST_SAVED_GNOMAD_VARIANT,
    TEST_SAVED_TAXONOMY,
    TEST_USER,
    VALID_CLINGEN_CA_ID,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.mocks.factories import create_mock_mapped_variant
from tests.helpers.util.common import (
    create_failing_side_effect,
    deepcamelize,
    parse_ndjson_response,
    update_expected_response_for_created_resources,
)
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.license import change_to_inactive_license
from tests.helpers.util.score_calibration import (
    create_publish_and_promote_score_calibration,
    create_test_score_calibration_in_score_set_via_client,
)
from tests.helpers.util.score_set import (
    create_seq_score_set,
    create_seq_score_set_with_mapped_variants,
    create_seq_score_set_with_variants,
    link_clinical_controls_to_mapped_variants,
    link_clinvar_control_to_mapped_variant,
    link_gnomad_variants_to_mapped_variants,
    publish_score_set,
)
from tests.helpers.util.user import change_ownership
from tests.helpers.util.variant import (
    clear_first_mapped_variant_post_mapped,
    create_mapped_variants_for_score_set,
    mock_worker_variant_insertion,
)

########################################################################################################################
# Score set schemas
########################################################################################################################


def test_TEST_MINIMAL_SEQ_SCORESET_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_SEQ_SCORESET, schema=ScoreSetCreate.model_json_schema())


def test_TEST_MINIMAL_ACC_SCORESET_is_valid():
    jsonschema.validate(instance=TEST_MINIMAL_ACC_SCORESET, schema=ScoreSetCreate.model_json_schema())


########################################################################################################################
# Score set creation
########################################################################################################################


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_create_minimal_score_set(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]

    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
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


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_create_score_set_with_contributor(client, mock_publication_fetch, setup_router_db):
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

    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
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
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_create_score_set_with_score_calibration(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update(
        {
            "scoreCalibrations": [deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)],
        }
    )

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 200
    response_data = response.json()

    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
    assert isinstance(MAVEDB_TMP_URN_RE.fullmatch(response_data["urn"]), re.Match)

    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, response_data
    )
    expected_response["experiment"].update({"numScoreSets": 1})
    expected_calibration = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    expected_calibration["urn"] = response_data["scoreCalibrations"][0]["urn"]
    expected_calibration["private"] = True
    expected_calibration["primary"] = False
    expected_calibration["investigatorProvided"] = True
    # Match functional classifications by a stable identifier (label).
    response_fcs_by_label = {
        fc["label"]: fc for fc in response_data["scoreCalibrations"][0]["functionalClassifications"]
    }
    for expected_fc in expected_calibration["functionalClassifications"]:
        label = expected_fc["label"]
        if label in response_fcs_by_label:
            expected_fc["id"] = response_fcs_by_label[label]["id"]

    expected_response["scoreCalibrations"] = [expected_calibration]

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    response = client.get(f"/api/v1/score-sets/{response_data['urn']}")
    assert response.status_code == 200


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_set_with_class_based_calibration(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update(
        {
            "scoreCalibrations": [deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED)],
        }
    )

    response = client.post("/api/v1/score-sets/", json=score_set)
    assert response.status_code == 409
    response_data = response.json()
    assert "Class-based calibrations are not supported on score set creation" in response_data["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_set_with_nonexistent_contributor(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set["experimentUrn"] = experiment["urn"]
    score_set.update({"contributors": [{"orcid_id": TEST_ORCID_ID}]})

    with patch(
        "mavedb.lib.orcid.fetch_orcid_user",
        side_effect=NonexistentOrcidUserError(f"No ORCID user was found for ORCID ID {TEST_ORCID_ID}."),
    ):
        response = client.post("/api/v1/score-sets/", json=score_set)

    assert response.status_code == 404
    response_data = response.json()
    assert "No ORCID user was found for ORCID ID 1111-1111-1111-1111." in response_data["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_set_without_email(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    client.put("api/v1/users/me", json={"email": None})
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 403
    response_data = response.json()
    assert response_data["detail"] in "There must be an email address associated with your account to use this feature."


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_set_with_invalid_target_gene_category(client, mock_publication_fetch, setup_router_db):
    experiment = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment["urn"]
    score_set_post_payload["targetGenes"][0]["category"] = "some_invalid_target_category"
    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 422
    response_data = response.json()
    assert "Input should be" in response_data["detail"][0]["msg"]
    assert all(field in response_data["detail"][0]["msg"] for field in TargetCategory._member_names_)


########################################################################################################################
# Score set supersession
########################################################################################################################


def _publish_score_set_owned_by_extra_user(session, data_provider, client, data_files):
    """Create and publish a score set, then reassign it to the extra user."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    change_ownership(session, published["urn"], ScoreSetDbModel)
    return published


def test_owner_can_supersede_own_published_score_set(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published["urn"]

    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 200
    assert response.json()["supersededScoreSet"]["urn"] == published["urn"]


def test_cannot_supersede_other_users_published_score_set(session, data_provider, client, setup_router_db, data_files):
    """A published score set may only be superseded by its owner or a contributor.

    Regression test: fetch_score_set_by_urn's owner_or_contributor filter previously admitted any
    non-private score set, which only_published already guaranteed, so this call was unauthorized.
    """
    published = _publish_score_set_owned_by_extra_user(session, data_provider, client, data_files)

    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published["urn"]

    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 404
    assert published["urn"] in response.json()["detail"]


def test_cannot_lock_owner_out_of_superseding_their_own_score_set(
    session, data_provider, client, setup_router_db, data_files
):
    """Supersession is one-shot, so an unauthorized claim would permanently block the real owner."""
    published = _publish_score_set_owned_by_extra_user(session, data_provider, client, data_files)

    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published["urn"]
    assert client.post("/api/v1/score-sets/", json=score_set_post_payload).status_code == 404

    # The owner's own supersession must still be available afterwards.
    response = client.get(f"/api/v1/score-sets/{published['urn']}")
    assert response.status_code == 200
    assert response.json().get("supersedingScoreSet") is None


def test_contributor_can_supersede_score_set(
    session, data_provider, client, setup_router_db, data_files, extra_user_app_overrides
):
    """A contributor to a published score set may record its successor, as well as its owner."""
    experiment = create_experiment(client, {"contributors": [{"orcidId": EXTRA_USER["username"]}]})
    score_set = create_seq_score_set(
        client, experiment["urn"], update={"contributors": [{"orcidId": EXTRA_USER["username"]}]}
    )
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = published["experiment"]["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published["urn"]

    # The extra user contributes to the score set but does not own it.
    with DependencyOverrider(extra_user_app_overrides):
        response = client.post("/api/v1/score-sets/", json=score_set_post_payload)

    assert response.status_code == 200
    assert response.json()["supersededScoreSet"]["urn"] == published["urn"]


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
        ("target_genes", TEST_MINIMAL_ACC_SCORESET["targetGenes"], TEST_MINIMAL_ACC_SCORESET_RESPONSE["targetGenes"]),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_can_patch_score_set_data_before_publication(
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

    data = {}
    if isinstance(updated_data, (dict, list)):
        form_value = json.dumps(updated_data)
    else:
        form_value = str(updated_data)
    data[attribute] = form_value

    response = client.patch(f"/api/v1/score-sets-with-variants/{score_set['urn']}", data=data)
    assert response.status_code == 200

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")
    assert response.status_code == 200
    response_data = response.json()

    # Although the client provides the license id, the response includes the full license.
    if attribute == "license_id":
        attribute = "license"

    assert expected_response_data == response_data[camelize(attribute)]


@pytest.mark.parametrize(
    "form_field,filename,mime_type",
    [
        ("scores_file", "scores.csv", "text/csv"),
        ("counts_file", "counts.csv", "text/csv"),
        ("score_columns_metadata_file", "score_columns_metadata.json", "application/json"),
        ("count_columns_metadata_file", "count_columns_metadata.json", "application/json"),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_can_patch_score_set_data_with_files_before_publication(
    client, setup_router_db, form_field, filename, mime_type, data_files, mock_publication_fetch, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    expected_response = update_expected_response_for_created_resources(
        deepcopy(TEST_MINIMAL_SEQ_SCORESET_RESPONSE), experiment, score_set
    )
    expected_response["experiment"].update({"numScoreSets": 1})

    if form_field == "counts_file" or form_field == "scores_file":
        data_file_path = data_files / filename
        files = {form_field: (filename, open(data_file_path, "rb"), mime_type)}
        with (
            patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue,
            patch.object(mock_s3_client, "upload_fileobj", return_value=None),
        ):
            response = client.patch(f"/api/v1/score-sets-with-variants/{score_set['urn']}", files=files)
            worker_queue.assert_called_once()
            assert response.status_code == 200
    elif form_field == "score_columns_metadata_file" or form_field == "count_columns_metadata_file":
        data_file_path = data_files / filename
        with open(data_file_path, "rb") as f:
            data = json.load(f)
            response = client.patch(f"/api/v1/score-sets-with-variants/{score_set['urn']}", data=data)
            assert response.status_code == 200


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
        ("dataset_columns", None, SAVED_MINIMAL_DATASET_COLUMNS),
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
            "datasetColumns": SAVED_MINIMAL_DATASET_COLUMNS,
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
        ("dataset_columns", {"countColumns": [], "scoreColumns": ["score"]}, SAVED_MINIMAL_DATASET_COLUMNS),
    ],
)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_set_target_data_after_publication(
    client,
    setup_router_db,
    attribute,
    expected_response_data,
    updated_data,
    session,
    data_provider,
    data_files,
    mock_publication_fetch,
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
            "datasetColumns": SAVED_MINIMAL_DATASET_COLUMNS,
            "processingState": ProcessingState.success.name,
        }
    )
    expected_response["experiment"].update({"numScoreSets": 1})

    assert sorted(expected_response.keys()) == sorted(response_data.keys())
    for key in expected_response:
        assert (key, expected_response[key]) == (key, response_data[key])

    score_set_update_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_update_payload.update(
        {
            camelize(attribute): updated_data,
        }
    )
    response = client.put(f"/api/v1/score-sets/{published_urn}", json=score_set_update_payload)
    assert response.status_code == 200

    response = client.get(f"/api/v1/score-sets/{published_urn}")
    assert response.status_code == 200
    response_data = response.json()

    if expected_response_data:
        assert expected_response_data == response_data[camelize(attribute)]
    else:
        assert camelize(attribute) not in response_data.keys()


def test_cannot_update_score_set_with_nonexistent_contributor(
    client,
    setup_router_db,
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    score_set_update_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_update_payload.update({"contributors": [{"orcid_id": TEST_ORCID_ID}]})

    with patch(
        "mavedb.lib.orcid.fetch_orcid_user",
        side_effect=NonexistentOrcidUserError(f"No ORCID user was found for ORCID ID {TEST_ORCID_ID}."),
    ):
        response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set_update_payload)

    assert response.status_code == 404
    response_data = response.json()
    assert "No ORCID user was found for ORCID ID 1111-1111-1111-1111." in response_data["detail"]


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
# Multiple score set fetching
########################################################################################################################


def test_get_score_sets_by_comma_separated_urns(client, setup_router_db):
    experiment = create_experiment(client)
    first_score_set = create_seq_score_set(client, experiment["urn"])
    second_score_set = create_seq_score_set(client, experiment["urn"])

    response = client.get(
        "/api/v1/score-sets/",
        params={"urns": f"{first_score_set['urn']}, {second_score_set['urn']}"},
    )
    assert response.status_code == 200

    response_data = response.json()
    assert [item["urn"] for item in response_data] == [first_score_set["urn"], second_score_set["urn"]]

    for item in response_data:
        jsonschema.validate(instance=item, schema=ScoreSet.model_json_schema())


def test_get_score_sets_requires_at_least_one_urn(client, setup_router_db):
    response = client.get("/api/v1/score-sets/", params={"urns": " , "})
    assert response.status_code == 422
    assert response.json()["detail"] == "At least one URN is required"


def test_get_score_sets_with_mixed_valid_and_invalid_urns_returns_404(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    missing_urn = "urn:mavedb:99999999-z-9"

    response = client.get(
        "/api/v1/score-sets/",
        params={"urns": f"{score_set['urn']},{missing_urn}"},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == f"score set with URN '{missing_urn}' not found"


def test_get_score_sets_with_whitespace_around_urns_in_mixed_list_returns_404(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    missing_urn = "urn:mavedb:99999999-z-9"

    response = client.get(
        "/api/v1/score-sets/",
        params={"urns": f"  {score_set['urn']}  ,   {missing_urn}   "},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == f"score set with URN '{missing_urn}' not found"


def test_show_score_sets_anonymous_can_fetch_public_score_sets(
    session, client, setup_router_db, anonymous_app_overrides, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(
            "/api/v1/score-sets/",
            params={"urns": published_score_set["urn"]},
        )

    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data) == 1
    assert response_data[0]["urn"] == published_score_set["urn"]


def test_show_score_sets_anonymous_cannot_fetch_private_score_sets(
    session, client, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    # Score set is private (not published); change ownership so it belongs to another user
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(
            "/api/v1/score-sets/",
            params={"urns": score_set["urn"]},
        )

    assert response.status_code == 404
    assert f"score set with URN '{score_set['urn']}' not found" in response.json()["detail"]


def test_show_score_sets_authenticated_user_can_fetch_own_private_score_sets(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    response = client.get(
        "/api/v1/score-sets/",
        params={"urns": score_set["urn"]},
    )

    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data) == 1
    assert response_data[0]["urn"] == score_set["urn"]


def test_show_score_sets_authenticated_user_cannot_fetch_other_users_private_score_sets(
    session, client, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(
        "/api/v1/score-sets/",
        params={"urns": score_set["urn"]},
    )

    assert response.status_code == 404
    assert f"score set with URN '{score_set['urn']}' not found" in response.json()["detail"]


def test_show_score_sets_mixed_public_and_private_returns_404(
    session, client, setup_router_db, anonymous_app_overrides, data_provider, data_files
):
    experiment = create_experiment(client)
    public_score_set = create_seq_score_set(client, experiment["urn"])
    public_score_set = mock_worker_variant_insertion(
        client, session, data_provider, public_score_set, data_files / "scores.csv"
    )
    private_score_set = create_seq_score_set(client, experiment["urn"])
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, public_score_set["urn"])
    # Make private_score_set belong to a different user to make it inaccessible anonymously
    change_ownership(session, private_score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(
            "/api/v1/score-sets/",
            params={"urns": f"{published_score_set['urn']},{private_score_set['urn']}"},
        )

    assert response.status_code == 404
    assert f"score set with URN '{private_score_set['urn']}' not found" in response.json()["detail"]


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


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_extra_user_can_only_view_published_score_calibrations_in_score_set(
    client, setup_router_db, extra_user_app_overrides, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    create_test_score_calibration_in_score_set_via_client(
        client, published_score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    public_calibration = create_publish_and_promote_score_calibration(
        client,
        published_score_set["urn"],
        deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED),
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data["scoreCalibrations"]) == 1
    assert response_data["scoreCalibrations"][0]["urn"] == public_calibration["urn"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        (
            [
                {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
                {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
            ]
        )
    ],
    indirect=["mock_publication_fetch"],
)
def test_creating_user_can_view_all_score_calibrations_in_score_set(client, setup_router_db, mock_publication_fetch):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    private_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    public_calibration = create_publish_and_promote_score_calibration(
        client,
        score_set["urn"],
        deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED),
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}")

    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data["scoreCalibrations"]) == 2
    urns = [calibration["urn"] for calibration in response_data["scoreCalibrations"]]
    assert private_calibration["urn"] in urns
    assert public_calibration["urn"] in urns


########################################################################################################################
# Adding scores to score set
########################################################################################################################


def test_add_score_set_variants_scores_only_endpoint(client, setup_router_db, data_files, mock_s3_client):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_and_counts_endpoint(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
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
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_counts_and_column_metadata_endpoint(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    score_columns_metadata_path = data_files / "score_columns_metadata.json"
    count_columns_metadata_path = data_files / "count_columns_metadata.json"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        open(score_columns_metadata_path, "rb") as score_columns_metadata_file,
        open(count_columns_metadata_path, "rb") as count_columns_metadata_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        score_columns_metadata = json.load(score_columns_metadata_file)
        count_columns_metadata = json.load(count_columns_metadata_file)
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
            data={
                "score_columns_metadata": json.dumps(score_columns_metadata),
                "count_columns_metadata": json.dumps(count_columns_metadata),
            },
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_only_endpoint_utf8_encoded(client, setup_router_db, data_files, mock_s3_client):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores_utf8_encoded.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_add_score_set_variants_scores_and_counts_endpoint_utf8_encoded(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores_utf8_encoded.csv"
    counts_csv_path = data_files / "counts_utf8_encoded.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
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
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

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
    assert response.status_code == 403
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


def test_contributor_can_add_scores_to_other_user_score_set(
    session, client, setup_router_db, data_files, mock_s3_client
):
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
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

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


def test_contributor_can_add_scores_and_counts_to_other_user_score_set(
    session, client, setup_router_db, data_files, mock_s3_client
):
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
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
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
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

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
    session, client, setup_router_db, data_files, mock_s3_client, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        DependencyOverrider(admin_app_overrides),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )
        queue.assert_called_once()

    assert response.status_code == 200
    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


def test_admin_can_add_scores_and_counts_to_other_user_score_set(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"
    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue,
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
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
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())

    # We test the worker process that actually adds the variant data separately. Here, we take it as
    # fact that it would have succeeded.
    score_set.update({"processingState": "processing"})
    assert score_set == response_data


########################################################################################################################
# Score set variant upload error handling
########################################################################################################################


def test_upload_score_set_variant_data_returns_500_and_resets_processing_state_when_enqueue_job_fails(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception("queue failure")),
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )

    assert response.status_code == 500

    db_score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])).one()
    session.refresh(db_score_set)
    assert db_score_set.processing_state == ProcessingState.failed

    pipelines = session.scalars(select(Pipeline).where(Pipeline.name == "validate_map_annotate_score_set")).all()
    assert pipelines == []


def test_upload_score_set_variant_data_deletes_s3_files_and_pipeline_when_enqueue_job_fails(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"
    counts_csv_path = data_files / "counts.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        open(counts_csv_path, "rb") as counts_file,
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception("queue failure")),
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={
                "scores_file": (scores_csv_path.name, scores_file, "text/csv"),
                "counts_file": (counts_csv_path.name, counts_file, "text/csv"),
            },
        )

    assert response.status_code == 500
    # Both uploaded S3 keys should be passed to delete_objects for cleanup.
    mock_s3_client.delete_objects.assert_called_once()
    delete_call_kwargs = mock_s3_client.delete_objects.call_args.kwargs
    deleted_keys = {obj["Key"] for obj in delete_call_kwargs["Delete"]["Objects"]}
    assert len(deleted_keys) == 2
    assert all("scores.csv" in k or "counts.csv" in k for k in deleted_keys)

    pipelines = session.scalars(select(Pipeline).where(Pipeline.name == "validate_map_annotate_score_set")).all()
    assert pipelines == []


def test_upload_score_set_variant_data_deletes_s3_files_when_pipeline_creation_fails(
    client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        patch("mavedb.routers.score_sets.PipelineFactory.create_pipeline", side_effect=Exception("pipeline failure")),
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.post(
            f"/api/v1/score-sets/{score_set['urn']}/variants/data",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )

    assert response.status_code == 500
    mock_s3_client.delete_objects.assert_called_once()
    delete_call_kwargs = mock_s3_client.delete_objects.call_args.kwargs
    deleted_keys = {obj["Key"] for obj in delete_call_kwargs["Delete"]["Objects"]}
    assert len(deleted_keys) == 1
    assert any("scores.csv" in k for k in deleted_keys)


def test_patch_score_set_with_variants_returns_500_and_resets_processing_state_when_enqueue_job_fails(
    session, client, setup_router_db, data_files, mock_s3_client
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    scores_csv_path = data_files / "scores.csv"

    with (
        open(scores_csv_path, "rb") as scores_file,
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception("queue failure")),
        patch.object(mock_s3_client, "upload_fileobj", return_value=None),
    ):
        response = client.patch(
            f"/api/v1/score-sets-with-variants/{score_set['urn']}",
            files={"scores_file": (scores_csv_path.name, scores_file, "text/csv")},
        )

    assert response.status_code == 500

    db_score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])).one()
    session.refresh(db_score_set)
    assert db_score_set.processing_state == ProcessingState.failed


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

    enqueue_args, enqueue_kwargs = worker_queue.call_args
    assert enqueue_args[0] == "start_pipeline"
    assert isinstance(enqueue_args[1], int)
    assert "_job_id" in enqueue_kwargs

    entrypoint_job = session.get(JobRun, enqueue_args[1])
    assert entrypoint_job is not None
    assert entrypoint_job.job_function == "start_pipeline"
    publish_pipeline = session.get(Pipeline, entrypoint_job.pipeline_id)
    assert publish_pipeline is not None
    assert publish_pipeline.name == "publish_score_set"

    refresh_job = session.scalars(
        select(JobRun).where(
            JobRun.pipeline_id == publish_pipeline.id,
            JobRun.job_function == "refresh_published_variants_view",
        )
    ).one()
    publish_job_functions = session.scalars(
        select(JobRun.job_function).where(JobRun.pipeline_id == publish_pipeline.id)
    ).all()
    assert sorted(publish_job_functions) == ["refresh_published_variants_view", "start_pipeline"]

    db_score_set = session.scalars(
        select(ScoreSetDbModel).where(ScoreSetDbModel.urn == published_score_set["urn"])
    ).one()
    assert refresh_job.job_params["correlation_id"] is not None
    assert refresh_job.job_params["score_set_id"] == db_score_set.id

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
            "datasetColumns": SAVED_MINIMAL_DATASET_COLUMNS,
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


def test_publish_score_set_discards_pipeline_when_entrypoint_enqueue_fails(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with (
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception("queue failure")) as worker_queue,
        patch("mavedb.routers.score_sets.send_slack_error") as mock_slack,
    ):
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()
        mock_slack.assert_called_once()

    assert isinstance(MAVEDB_SCORE_SET_URN_RE.fullmatch(published_score_set["urn"]), re.Match)

    pipelines = session.scalars(select(Pipeline).where(Pipeline.name == "publish_score_set")).all()
    assert pipelines == []


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


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_score_calibrations_remain_private_when_score_set_is_published(
    session, data_provider, client, setup_router_db, data_files, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(
        client,
        experiment["urn"],
    )
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    # refresh score set to post worker state
    score_set = (client.get(f"/api/v1/score-sets/{published_score_set['urn']}")).json()

    for score_calibration in score_set["scoreCalibrations"]:
        assert score_calibration["private"] is True


def test_cannot_publish_score_set_without_variants(client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 409
        worker_queue.assert_not_called()
        response_data = response.json()

    assert "cannot publish score set without variant scores" in response_data["detail"]


########################################################################################################################
# Recently published score sets
########################################################################################################################


def test_recently_published_returns_empty_list_when_no_score_sets_published(client, setup_router_db):
    response = client.get("/api/v1/score-sets/recently-published")
    assert response.status_code == 200
    assert response.json() == []


def test_recently_published_returns_published_score_sets(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set_1 = create_seq_score_set(client, experiment["urn"])
    score_set_1 = mock_worker_variant_insertion(client, session, data_provider, score_set_1, data_files / "scores.csv")
    score_set_2 = create_seq_score_set(client, experiment["urn"])
    score_set_2 = mock_worker_variant_insertion(client, session, data_provider, score_set_2, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_1 = publish_score_set(client, score_set_1["urn"])
        published_2 = publish_score_set(client, score_set_2["urn"])

    response = client.get("/api/v1/score-sets/recently-published")
    assert response.status_code == 200
    response_data = response.json()

    returned_urns = [ss["urn"] for ss in response_data]
    assert published_1["urn"] in returned_urns
    assert published_2["urn"] in returned_urns


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_recently_published_withholds_private_calibrations_from_anonymous_users(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides, mock_publication_fetch
):
    """A published score set can carry an unpublished calibration.

    This endpoint checks READ on the score set and on its superseding score set, but a calibration's READ
    rule is stricter than its score set's, so it needs its own filter. Without it the listing served every
    private calibration's thresholds to anyone.
    """
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    # The owner sees their own private calibration.
    owner_response = client.get("/api/v1/score-sets/recently-published")
    assert owner_response.status_code == 200
    owner_entry = next(ss for ss in owner_response.json() if ss["urn"] == published["urn"])
    assert len(owner_entry.get("scoreCalibrations") or []) == 1

    with DependencyOverrider(anonymous_app_overrides):
        anonymous_response = client.get("/api/v1/score-sets/recently-published")

    assert anonymous_response.status_code == 200
    anonymous_entry = next(ss for ss in anonymous_response.json() if ss["urn"] == published["urn"])
    assert (anonymous_entry.get("scoreCalibrations") or []) == []


def test_recently_published_does_not_return_unpublished_score_sets(client, setup_router_db):
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])

    response = client.get("/api/v1/score-sets/recently-published")
    assert response.status_code == 200
    assert response.json() == []


def test_recently_published_respects_limit_parameter(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    # Create and upload variants for all score sets before publishing any, because publishing
    # changes the experiment URN from a tmp URN to a permanent URN.
    score_sets = []
    for _ in range(3):
        score_set = create_seq_score_set(client, experiment["urn"])
        score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
        score_sets.append(score_set)

    published_urns = []
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        for score_set in score_sets:
            published = publish_score_set(client, score_set["urn"])
            published_urns.append(published["urn"])

    response = client.get("/api/v1/score-sets/recently-published?limit=2")
    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data) == 2


def test_recently_published_rejects_limit_exceeding_maximum(client, setup_router_db):
    response = client.get("/api/v1/score-sets/recently-published?limit=21")
    assert response.status_code == 422


def test_recently_published_rejects_limit_of_zero(client, setup_router_db):
    response = client.get("/api/v1/score-sets/recently-published?limit=0")
    assert response.status_code == 422


def test_recently_published_accessible_to_anonymous_user(
    session, data_provider, client, setup_router_db, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/score-sets/recently-published")

    assert response.status_code == 200
    returned_urns = [ss["urn"] for ss in response.json()]
    assert published["urn"] in returned_urns


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


def test_contributor_cannot_publish_other_users_score_set(session, data_provider, client, setup_router_db, data_files):
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
        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert response.status_code == 403
        worker_queue.assert_not_called()
        response_data = response.json()

    assert f"insufficient permissions on score set with URN '{score_set['urn']}'" in response_data["detail"]


def test_admin_can_publish_other_user_private_score_set(
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
        assert response.status_code == 200
        queue.assert_called_once()


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
    jsonschema.validate(instance=score_set_2, schema=ScoreSetCreate.model_json_schema())

    response = client.post("/api/v1/score-sets/", json=score_set_2)
    response_data = response.json()
    assert response.status_code == 409
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
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


def test_search_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["title"] == score_set["title"]


def test_search_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")

    search_payload = {"text": score_set["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == score_set["urn"]


def test_search_private_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")

    urn_with_space = score_set["urn"] + "   "
    search_payload = {"text": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == score_set["urn"]


def test_search_others_private_score_sets_no_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


def test_search_others_private_score_sets_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    change_ownership(session, score_set["urn"], ScoreSetDbModel)
    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


def test_search_others_private_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    search_payload = {"text": score_set["urn"]}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


def test_search_others_private_score_sets_urn_with_space_match(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    urn_with_space = score_set["urn"] + "   "
    search_payload = {"text": urn_with_space}
    response = client.post("/api/v1/me/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


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
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


def test_search_public_score_sets_does_not_disclose_private_sibling_urns(
    session, data_provider, client, anonymous_app_overrides, setup_router_db, data_files
):
    """A search result's experiment lists only the score set URNs the caller may read.

    Regression test: the enrichment that filters those URNs used to be skipped when the request set
    includeExperimentScoreSetUrnsAndCount to false, and SavedExperiment's validator then listed every score
    set on the experiment. The field is gone, and an unknown field is ignored rather than rejected, so the
    old request shape must now be filtered too.
    """
    experiment = create_experiment(client, {"title": "Experiment 1"})
    published = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    published = mock_worker_variant_insertion(client, session, data_provider, published, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, published["urn"])

    # Unpublished, and inside the now-public experiment. This is the URN that leaked.
    private = create_seq_score_set(
        client, published["experiment"]["urn"], update={"title": "Unpublished Fnord Score Set"}
    )

    # The owner may read both, so the enrichment is permission-scoped rather than a blanket strip.
    response = client.post("/api/v1/score-sets/search", json={"text": "fnord"})
    assert response.status_code == 200
    assert set(response.json()["scoreSets"][0]["experiment"]["scoreSetUrns"]) == {published["urn"], private["urn"]}

    for search_payload in (
        {"text": "fnord"},
        {"text": "fnord", "includeExperimentScoreSetUrnsAndCount": False},
    ):
        with DependencyOverrider(anonymous_app_overrides):
            response = client.post("/api/v1/score-sets/search", json=search_payload)

        assert response.status_code == 200
        assert len(response.json()["scoreSets"]) == 1

        score_set_urns = response.json()["scoreSets"][0]["experiment"]["scoreSetUrns"]
        assert published["urn"] in score_set_urns
        assert private["urn"] not in score_set_urns


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
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["title"] == score_set["title"]


def test_cannot_search_public_score_sets_with_published_false(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord", "published": "false"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert (
        "Cannot search for private score sets except in the context of the current user's data."
        in response_data["detail"]
    )


def test_search_public_score_sets_invalid_limit(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord", "limit": 101}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert (
        "Cannot search for more than 100 score sets at a time. Please use the offset and limit parameters to run a paginated search."
        in response_data["detail"]
    )


def test_search_public_score_sets_valid_limit(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord", "limit": 100}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["title"] == score_set["title"]


def test_search_public_score_sets_too_many_publication_identifiers(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    publication_identifier_search = [str(20711194 + i) for i in range(41)]
    search_payload = {"text": "fnord", "publication_identifiers": publication_identifier_search}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    response_data = response.json()
    assert response.status_code == 422
    assert (
        "Cannot search for score sets belonging to more than 40 publication identifiers at once."
        in response_data["detail"]
    )


def test_search_public_score_sets_urn_with_space_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        publish_score_set(client, decoy["urn"])
        assert worker_queue.call_count == 2

    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"text": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == published_score_set["urn"]


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
    assert response.json()["numScoreSets"] == 0
    assert len(response.json()["scoreSets"]) == 0


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
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["title"] == published_score_set["title"]


def test_search_others_public_score_sets_urn_match(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        publish_score_set(client, decoy["urn"])
        assert worker_queue.call_count == 2

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    search_payload = {"text": published_score_set["urn"]}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == published_score_set["urn"]


def test_search_others_public_score_sets_urn_with_space_match(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 1"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    decoy = create_seq_score_set(client, experiment["urn"], update={"title": "Score Set 2"})
    decoy = mock_worker_variant_insertion(client, session, data_provider, decoy, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        publish_score_set(client, decoy["urn"])
        assert worker_queue.call_count == 2

    change_ownership(session, published_score_set["urn"], ScoreSetDbModel)
    urn_with_space = published_score_set["urn"] + "   "
    search_payload = {"text": urn_with_space}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == published_score_set["urn"]


def test_cannot_search_private_score_sets(session, data_provider, client, setup_router_db, data_files):
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
    assert response.status_code == 422

    response_data = response.json()
    assert (
        "Cannot search for private score sets except in the context of the current user's data."
        in response_data["detail"]
    )


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
    assert response.json()["numScoreSets"] == 1
    assert len(response.json()["scoreSets"]) == 1
    assert response.json()["scoreSets"][0]["urn"] == published_score_set_1["urn"]


def test_search_published_score_set_not_hidden_by_unpublished_superseding_version(
    session, data_provider, client, setup_router_db, data_files
):
    """A published score set should still appear in search results when its superseding version is unpublished."""
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    # Create an unpublished superseding score set — this should NOT hide the published precursor.
    create_seq_score_set(
        client,
        published_score_set["experiment"]["urn"],
        update={"supersededScoreSetUrn": published_score_set["urn"]},
    )

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert response.json()["numScoreSets"] == 1
    assert response.json()["scoreSets"][0]["urn"] == published_score_set["urn"]


def test_search_published_score_set_hidden_by_published_superseding_version(
    session, data_provider, client, setup_router_db, data_files
):
    """A published score set should be hidden from search results when its superseding version is also published."""
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Test Fnord Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    # Create and publish a superseding score set — this SHOULD hide the precursor.
    superseding = create_seq_score_set(
        client,
        published_score_set["experiment"]["urn"],
        update={"title": "Test Fnord Score Set v2", "supersededScoreSetUrn": published_score_set["urn"]},
    )
    superseding = mock_worker_variant_insertion(client, session, data_provider, superseding, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_superseding = publish_score_set(client, superseding["urn"])
        worker_queue.assert_called_once()

    search_payload = {"text": "fnord"}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    # Only the superseding version should appear; the precursor should be hidden.
    assert response.json()["numScoreSets"] == 1
    assert response.json()["scoreSets"][0]["urn"] == published_superseding["urn"]


def test_search_filter_options_not_hidden_by_unpublished_superseding_version(
    session, data_provider, client, setup_router_db, data_files
):
    """Filter options should include targets from published score sets even when superseded by unpublished versions."""
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    target_name = published_score_set["targetGenes"][0]["name"]

    # Create an unpublished superseding version.
    create_seq_score_set(
        client,
        published_score_set["experiment"]["urn"],
        update={"supersededScoreSetUrn": published_score_set["urn"]},
    )

    response = client.post("/api/v1/score-sets/search/filter-options", json={})
    assert response.status_code == 200
    target_names = [opt["value"] for opt in response.json()["targetGeneNames"]]
    assert target_name in target_names


def test_search_filter_options_hidden_by_published_superseding_version(
    session, data_provider, client, setup_router_db, data_files
):
    """Filter options should NOT include targets from published score sets when superseded by published versions."""
    experiment = create_experiment(client, {"title": "Experiment 1"})
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    target_name = published_score_set["targetGenes"][0]["name"]

    # Create and publish a superseding version.
    superseding = create_seq_score_set(
        client,
        published_score_set["experiment"]["urn"],
        update={
            "supersededScoreSetUrn": published_score_set["urn"],
            "targetGenes": [
                {
                    "name": "TEST2",
                    "category": "protein_coding",
                    "externalIdentifiers": [],
                    "targetSequence": {
                        "sequenceType": "dna",
                        "sequence": "ACGTTT",
                        "taxonomy": {
                            "code": TEST_SAVED_TAXONOMY["code"],
                            "organismName": TEST_SAVED_TAXONOMY["organism_name"],
                            "commonName": TEST_SAVED_TAXONOMY["common_name"],
                            "rank": TEST_SAVED_TAXONOMY["rank"],
                            "hasDescribedSpeciesName": TEST_SAVED_TAXONOMY["has_described_species_name"],
                            "articleReference": TEST_SAVED_TAXONOMY["article_reference"],
                            "id": TEST_SAVED_TAXONOMY["id"],
                            "url": TEST_SAVED_TAXONOMY["url"],
                        },
                    },
                }
            ],
        },
    )
    superseding = mock_worker_variant_insertion(client, session, data_provider, superseding, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, superseding["urn"])
        worker_queue.assert_called_once()

    response = client.post("/api/v1/score-sets/search/filter-options", json={})
    assert response.status_code == 200
    target_names = [opt["value"] for opt in response.json()["targetGeneNames"]]
    assert target_name not in target_names


def test_search_score_sets_reports_correct_total_count_with_limit(
    session, data_provider, client, setup_router_db, data_files
):
    """When more published score sets exist than the search limit, num_score_sets should reflect the true total."""
    num_score_sets = 3
    for i in range(num_score_sets):
        experiment = create_experiment(client, {"title": f"Experiment {i}"})
        score_set = create_seq_score_set(client, experiment["urn"], update={"title": f"Score Set {i}"})
        score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            publish_score_set(client, score_set["urn"])

    search_payload = {"limit": 2}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()["scoreSets"]) == 2
    assert response.json()["numScoreSets"] == num_score_sets


def test_search_score_sets_not_affected_by_experiment_metadata(
    session, data_provider, client, setup_router_db, data_files
):
    """Experiments with multiple keywords should not reduce the number of score sets returned by search."""
    num_score_sets = 3
    for i in range(num_score_sets):
        experiment = create_experiment(client, {"keywords": TEST_KEYWORDS, "title": f"Experiment {i}"})
        score_set = create_seq_score_set(client, experiment["urn"], update={"title": f"Score Set {i}"})
        score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            publish_score_set(client, score_set["urn"])

    search_payload = {"limit": 2}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()["scoreSets"]) == 2
    assert response.json()["numScoreSets"] == num_score_sets


def test_cannot_create_multiple_superseding_versions(session, data_provider, client, setup_router_db, data_files):
    """Attempting to create multiple superseding versions should fail."""
    experiment = create_experiment(client, {"title": "Original Experiment"})
    score_set = create_seq_score_set(client, experiment["urn"], update={"title": "Original Score Set"})
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    # Create the first private superseding score set successfully
    first_superseding = create_seq_score_set(
        client,
        create_experiment(client, {"title": "First Superseding Experiment"})["urn"],
        update={"title": "First Superseding", "supersededScoreSetUrn": published["urn"]},
    )
    assert first_superseding is not None

    # Attempt to create the second private superseding score set for the same published score set
    # This should fail due to the unique constraint on replaces_id
    experiment2 = create_experiment(client)
    score_set_post_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    score_set_post_payload["experimentUrn"] = experiment2["urn"]
    score_set_post_payload["supersededScoreSetUrn"] = published["urn"]

    response = client.post("/api/v1/score-sets/", json=score_set_post_payload)
    assert response.status_code == 409
    assert (f"This score set has been superseded by score set: {first_superseding['urn']}.") in response.json()[
        "detail"
    ]


def test_search_score_sets_not_affected_by_an_unpublishing_superseding_versions(
    session, data_provider, client, setup_router_db, data_files
):
    """One unpublished superseding versions of the same score set should not reduce search page size.

    Regression test for a bug where the superseding score set filter used a LEFT OUTER JOIN
    (scoresets LEFT JOIN scoresets AS s ON scoresets.id = s.replaces_id). replaces_id has
    uniqueness constraint now.
    """
    num_published = 3
    published_urns = []
    for i in range(num_published):
        experiment = create_experiment(client, {"title": f"Experiment {i}"})
        score_set = create_seq_score_set(client, experiment["urn"], update={"title": f"Score Set {i}"})
        score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, score_set["urn"])
        published_urns.append(published["urn"])

    # Create an unpublished superseding versions for the first score set.
    create_seq_score_set(
        client,
        create_experiment(client, {"title": f"Superseding Experiment {1}"})["urn"],
        update={"title": f"Superseding {1}", "supersededScoreSetUrn": published_urns[0]},
    )

    search_payload = {"limit": 2}
    response = client.post("/api/v1/score-sets/search", json=search_payload)
    assert response.status_code == 200
    assert len(response.json()["scoreSets"]) == 2
    assert response.json()["numScoreSets"] == num_published


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
    assert (
        f"insufficient permissions on score set with URN '{published_score_set['urn']}'" in del_response_data["detail"]
    )


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

    assert response.status_code == 403
    response_data = response.json()
    assert f"insufficient permissions on score set with URN '{score_set['urn']}'" in response_data["detail"]


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
    assert response.status_code == 409


def test_cannot_modify_score_set_to_inactive_license(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set_post_payload = score_set.copy()
    score_set_post_payload.update({"licenseId": TEST_INACTIVE_LICENSE["id"], "urn": score_set["urn"]})
    response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set_post_payload)
    assert response.status_code == 409


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
    assert response.status_code == 422
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
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?drop_unused_hgvs_columns=true&namespaces=scores&namespaces=mavedb"
    )
    assert download_scores_csv_response.status_code == 200
    download_scores_csv = download_scores_csv_response.text

    reader = csv.DictReader(StringIO(download_scores_csv))
    assert sorted(reader.fieldnames) == sorted(
        [
            "accession",
            "hgvs_nt",
            "hgvs_pro",
            "mavedb.post_mapped_hgvs_g",
            "mavedb.post_mapped_hgvs_p",
            "mavedb.post_mapped_hgvs_c",
            "mavedb.post_mapped_hgvs_at_assay_level",
            "mavedb.post_mapped_vrs_id",
            "scores.score",
        ]
    )
    rows = list(reader)
    for row in rows:
        if has_hgvs_g:
            assert row["mavedb.post_mapped_hgvs_g"] == mapped_variant["hgvs_g"]
            assert row["mavedb.post_mapped_hgvs_c"] == mapped_variant["hgvs_c"]
            assert row["mavedb.post_mapped_hgvs_p"] == mapped_variant["hgvs_p"]
            assert row["mavedb.post_mapped_hgvs_at_assay_level"] == mapped_variant["hgvs_assay_level"]
        elif has_hgvs_p:
            assert row["mavedb.post_mapped_hgvs_g"] == "NA"
            assert row["mavedb.post_mapped_hgvs_c"] == "NA"
            assert row["mavedb.post_mapped_hgvs_p"] == mapped_variant["hgvs_p"]
            assert row["mavedb.post_mapped_hgvs_at_assay_level"] == mapped_variant["hgvs_assay_level"]
        else:
            assert row["mavedb.post_mapped_hgvs_g"] == "NA"
            assert row["mavedb.post_mapped_hgvs_c"] == "NA"
            assert row["mavedb.post_mapped_hgvs_p"] == "NA"
            assert row["mavedb.post_mapped_hgvs_at_assay_level"] == "NA"


# Test file doesn't have hgvs_splice so its values are all NA.
def test_download_scores_file(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_scores_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/scores?drop_unused_hgvs_columns=true"
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
        f"/api/v1/score-sets/{published_score_set['urn']}/counts?drop_unused_hgvs_columns=true"
    )
    assert download_counts_csv_response.status_code == 200
    download_counts_csv = download_counts_csv_response.text
    csv_header = download_counts_csv.split("\n")[0]
    columns = csv_header.split(",")
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns


# Deprecated query-parameter aliases. Galaxy and other external tooling call these endpoints, so the old
# names keep working for a release rather than being silently ignored.
def test_deprecated_drop_na_columns_still_drops_unused_hgvs_columns(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    for path in ("variants/data?namespaces=scores&", "scores?", "counts?"):
        response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}/{path}drop_na_columns=true")

        assert response.status_code == 200, path
        columns = response.text.split("\n")[0].split(",")
        assert "hgvs_splice" not in columns, path


def test_deprecated_include_post_mapped_hgvs_adds_the_mavedb_namespace(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    create_mapped_variants_for_score_set(session, score_set["urn"], TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION)
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&include_post_mapped_hgvs=true"
    )

    assert response.status_code == 200
    columns = response.text.split("\n")[0].split(",")
    # Additive, as the flag always was: the requested namespace survives alongside it.
    assert "scores.score" in columns
    assert "mavedb.post_mapped_hgvs_g" in columns


def test_deprecated_include_custom_columns_adds_the_scores_custom_namespace(
    session, data_provider, client, setup_router_db, data_files
):
    """The flag now appends a namespace, and its columns keep the `scores.` prefix they always had."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    with_flag = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&include_custom_columns=true"
    )
    with_namespace = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&namespaces=scores_custom"
    )

    assert with_flag.status_code == 200
    assert with_namespace.status_code == 200
    assert with_flag.text.split("\n")[0] == with_namespace.text.split("\n")[0]
    assert with_flag.headers["Deprecation"] == "true"
    assert "include_custom_columns is deprecated" in with_flag.headers["Warning"]
    # No column is emitted under a `scores_custom.` prefix; the namespace is a request token only.
    assert "scores_custom." not in with_flag.text


def test_current_parameter_name_wins_over_its_deprecated_spelling(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/scores?drop_unused_hgvs_columns=false&drop_na_columns=true"
    )

    assert response.status_code == 200
    assert "hgvs_splice" in response.text.split("\n")[0].split(",")


def test_deprecated_request_answers_with_deprecation_headers(
    session, data_provider, client, setup_router_db, data_files
):
    """The consumers here are scripts, not people reading our logs, so the response has to say so."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data"
        "?namespaces=scores&drop_na_columns=true&include_post_mapped_hgvs=true"
    )

    assert response.status_code == 200
    assert response.headers["deprecation"] == "true"
    warning = response.headers["warning"]
    assert "drop_na_columns is deprecated, use drop_unused_hgvs_columns" in warning
    assert "include_post_mapped_hgvs is deprecated, use namespaces=mavedb" in warning


def test_current_request_carries_no_deprecation_headers(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published_score_set = publish_score_set(client, score_set["urn"])

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&drop_unused_hgvs_columns=true"
    )

    assert response.status_code == 200
    assert "deprecation" not in response.headers
    assert "warning" not in response.headers


def test_deprecated_parameters_are_marked_deprecated_in_the_openapi_schema(client):
    """Anyone reading the docs or generating a client should see the deprecation without sending a request."""
    schema = client.app.openapi()

    def parameter(path: str, name: str):
        return next(p for p in schema["paths"][path]["get"]["parameters"] if p["name"] == name)

    for path, name in (
        ("/api/v1/score-sets/{urn}/variants/data", "drop_na_columns"),
        ("/api/v1/score-sets/{urn}/variants/data", "include_post_mapped_hgvs"),
        ("/api/v1/score-sets/{urn}/scores", "drop_na_columns"),
        ("/api/v1/score-sets/{urn}/counts", "drop_na_columns"),
    ):
        assert parameter(path, name)["deprecated"] is True, f"{name} on {path}"
        assert "deprecated" in parameter(path, name)["description"].lower(), f"{name} on {path}"


# Namespace variant CSV export tests.
def test_download_scores_file_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_scores_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&drop_unused_hgvs_columns=true"
    )
    assert download_scores_csv_response.status_code == 200
    download_scores_csv = download_scores_csv_response.text
    reader = csv.reader(StringIO(download_scores_csv))
    columns = next(reader)
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns
    assert "scores.score" in columns


def test_download_counts_file_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_counts_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=counts&include_custom_columns=true&drop_unused_hgvs_columns=true"
    )
    assert download_counts_csv_response.status_code == 200
    download_counts_csv = download_counts_csv_response.text
    reader = csv.reader(StringIO(download_counts_csv))
    columns = next(reader)
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns
    assert "counts.c_0" in columns
    assert "counts.c_1" in columns


def test_download_scores_and_counts_file(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_scores_and_counts_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=counts&namespaces=scores&include_custom_columns=true&drop_unused_hgvs_columns=true"
    )
    assert download_scores_and_counts_csv_response.status_code == 200
    download_scores_and_counts_csv = download_scores_and_counts_csv_response.text
    reader = csv.DictReader(StringIO(download_scores_and_counts_csv))
    assert sorted(reader.fieldnames) == sorted(
        ["accession", "hgvs_nt", "hgvs_pro", "scores.score", "scores.s_0", "scores.s_1", "counts.c_0", "counts.c_1"]
    )


@pytest.mark.parametrize(
    "mapped_variant,has_hgvs_g,has_hgvs_p",
    [
        (None, False, False),
        (TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION, True, False),
        (TEST_MAPPED_VARIANT_WITH_HGVS_P_EXPRESSION, False, True),
    ],
    ids=["without_post_mapped_vrs", "with_post_mapped_hgvs_g", "with_post_mapped_hgvs_p"],
)
def test_download_scores_counts_and_post_mapped_variants_file(
    session, data_provider, client, setup_router_db, data_files, mapped_variant, has_hgvs_g, has_hgvs_p
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    if mapped_variant is not None:
        create_mapped_variants_for_score_set(session, score_set["urn"], mapped_variant)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    download_multiple_data_csv_response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&namespaces=counts&namespaces=mavedb&include_custom_columns=true&drop_unused_hgvs_columns=true"
    )
    assert download_multiple_data_csv_response.status_code == 200
    download_multiple_data_csv = download_multiple_data_csv_response.text
    reader = csv.DictReader(StringIO(download_multiple_data_csv))
    assert sorted(reader.fieldnames) == sorted(
        [
            "accession",
            "hgvs_nt",
            "hgvs_pro",
            "mavedb.post_mapped_hgvs_c",
            "mavedb.post_mapped_hgvs_g",
            "mavedb.post_mapped_hgvs_p",
            "mavedb.post_mapped_hgvs_at_assay_level",
            "mavedb.post_mapped_vrs_id",
            "scores.score",
            "scores.s_0",
            "scores.s_1",
            "counts.c_0",
            "counts.c_1",
        ]
    )


# Additional namespace export tests: VEP, ClinGen, gnomAD
def test_download_vep_file_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    # Create mapped variants with VEP consequence populated
    create_mapped_variants_for_score_set(session, score_set["urn"], TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=vep&drop_unused_hgvs_columns=true"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert "vep.vep_functional_consequence" in reader.fieldnames
    # At least one row should contain the test consequence value
    rows = list(reader)
    assert any(row.get("vep.vep_functional_consequence") == "missense_variant" for row in rows)


def test_download_clingen_file_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    # Create mapped variants then set ClinGen allele id for first mapped variant
    create_mapped_variants_for_score_set(session, score_set["urn"], TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION)
    db_score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set["urn"]).one()
    first_mapped_variant = db_score_set.variants[0].mapped_variants[0]
    first_mapped_variant.clingen_allele_id = VALID_CLINGEN_CA_ID
    session.add(first_mapped_variant)
    session.commit()

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=clingen&drop_unused_hgvs_columns=true"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert "clingen.clingen_allele_id" in reader.fieldnames
    rows = list(reader)
    assert rows[0].get("clingen.clingen_allele_id") == VALID_CLINGEN_CA_ID


def test_download_gnomad_file_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    experiment = create_experiment(client)
    # Link a gnomAD variant to the first mapped variant (version may not match export filter)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_gnomad_variants_to_mapped_variants(session, score_set)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=gnomad&drop_unused_hgvs_columns=true"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert "gnomad.gnomad_af" in reader.fieldnames


def test_download_gnomad_file_keeps_variants_linked_to_other_gnomad_versions(
    session, data_provider, client, setup_router_db, data_files
):
    """A variant linked only to a gnomAD record of another version must still appear, with an NA frequency.

    The version filter belongs in the join's ON clause; in a WHERE it silently drops the variant row.
    """
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    # The seeded gnomAD variant's version deliberately differs from the configured export version.
    link_gnomad_variants_to_mapped_variants(session, score_set)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=gnomad")
    assert response.status_code == 200

    rows = list(csv.DictReader(StringIO(response.text)))
    assert len(rows) == 3, "every variant must be present regardless of linked gnomAD versions"
    assert all(row["gnomad.gnomad_af"] == "NA" for row in rows)


def test_download_clingen_and_vep_file_in_variant_data_path(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    # Create mapped variants with VEP consequence populated
    create_mapped_variants_for_score_set(session, score_set["urn"], TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION)
    db_score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set["urn"]).one()
    first_mapped_variant = db_score_set.variants[0].mapped_variants[0]
    first_mapped_variant.clingen_allele_id = VALID_CLINGEN_CA_ID
    session.add(first_mapped_variant)
    session.commit()

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=clingen&namespaces=vep&drop_unused_hgvs_columns=true"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert "vep.vep_functional_consequence" in reader.fieldnames
    assert "clingen.clingen_allele_id" in reader.fieldnames
    rows = list(reader)
    assert any(row.get("vep.vep_functional_consequence") == "missense_variant" for row in rows)
    assert rows[0].get("clingen.clingen_allele_id") == VALID_CLINGEN_CA_ID


def test_download_clingen_and_scores_file_in_variant_data_path(
    session, data_provider, client, setup_router_db, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(
        client, session, data_provider, score_set, data_files / "scores.csv", data_files / "counts.csv"
    )
    # Create mapped variants with VEP consequence populated
    create_mapped_variants_for_score_set(session, score_set["urn"], TEST_MAPPED_VARIANT_WITH_HGVS_G_EXPRESSION)
    db_score_set = session.query(ScoreSetDbModel).filter(ScoreSetDbModel.urn == score_set["urn"]).one()
    first_mapped_variant = db_score_set.variants[0].mapped_variants[0]
    first_mapped_variant.clingen_allele_id = VALID_CLINGEN_CA_ID
    session.add(first_mapped_variant)
    session.commit()

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data?namespaces=scores&namespaces=clingen&drop_unused_hgvs_columns=true"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert "clingen.clingen_allele_id" in reader.fieldnames
    rows = list(reader)
    assert rows[0].get("clingen.clingen_allele_id") == VALID_CLINGEN_CA_ID
    download_multiple_data_csv = response.text
    reader = csv.DictReader(StringIO(download_multiple_data_csv))
    assert sorted(reader.fieldnames) == sorted(
        [
            "accession",
            "clingen.clingen_allele_id",
            "hgvs_nt",
            "hgvs_pro",
            "scores.score",
        ]
    )


def test_download_clinvar_namespace_in_variant_data_path(session, data_provider, client, setup_router_db, data_files):
    """ClinVar namespace returns clinical_significance and clinical_review_status columns with correct values."""
    # The ClinVar control seeded in setup_router_db has db_version="11_2024", mapping to namespace clinvar.2024_11.
    clinvar_namespace = "clinvar.2024_11"
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinvar_control_to_mapped_variant(session, score_set)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data"
        f"?namespaces={clinvar_namespace}&drop_unused_hgvs_columns=false"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert f"{clinvar_namespace}.clinical_significance" in reader.fieldnames
    assert f"{clinvar_namespace}.clinical_review_status" in reader.fieldnames

    rows = list(reader)
    # The first variant is linked to the ClinVar control; check its values.
    assert rows[0][f"{clinvar_namespace}.clinical_significance"] == TEST_CLINVAR_CONTROL["clinical_significance"]
    assert rows[0][f"{clinvar_namespace}.clinical_review_status"] == TEST_CLINVAR_CONTROL["clinical_review_status"]
    # Other variants have no linked control for this version; they should be NA.
    assert all(row[f"{clinvar_namespace}.clinical_significance"] == "NA" for row in rows[1:])
    assert all(row[f"{clinvar_namespace}.clinical_review_status"] == "NA" for row in rows[1:])


def test_download_clinvar_namespace_with_no_matching_version(
    session, data_provider, client, setup_router_db, data_files
):
    """When no controls match the requested ClinVar version, all rows return NA."""
    # clinvar.2023_01 does not match the seeded control (11_2024), so all rows should be NA.
    clinvar_namespace = "clinvar.2023_01"
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinvar_control_to_mapped_variant(session, score_set)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data"
        f"?namespaces={clinvar_namespace}&drop_unused_hgvs_columns=false"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    assert f"{clinvar_namespace}.clinical_significance" in reader.fieldnames
    assert f"{clinvar_namespace}.clinical_review_status" in reader.fieldnames

    rows = list(reader)
    assert all(row[f"{clinvar_namespace}.clinical_significance"] == "NA" for row in rows)
    assert all(row[f"{clinvar_namespace}.clinical_review_status"] == "NA" for row in rows)


def test_download_multiple_clinvar_namespaces_in_variant_data_path(
    session, data_provider, client, setup_router_db, data_files
):
    """Multiple ClinVar namespaces produce distinct column sets; only the matching version has real data."""
    matching_ns = "clinvar.2024_11"  # matches db_version="11_2024" seeded in setup_router_db
    non_matching_ns = "clinvar.2023_01"  # no controls with this version
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinvar_control_to_mapped_variant(session, score_set)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        published_score_set = publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(
        f"/api/v1/score-sets/{published_score_set['urn']}/variants/data"
        f"?namespaces={matching_ns}&namespaces={non_matching_ns}&drop_unused_hgvs_columns=false"
    )
    assert response.status_code == 200
    reader = csv.DictReader(StringIO(response.text))
    fieldnames = reader.fieldnames
    # Both namespaces produce columns.
    assert f"{matching_ns}.clinical_significance" in fieldnames
    assert f"{matching_ns}.clinical_review_status" in fieldnames
    assert f"{non_matching_ns}.clinical_significance" in fieldnames
    assert f"{non_matching_ns}.clinical_review_status" in fieldnames

    rows = list(reader)
    # Matching version: first variant has data.
    assert rows[0][f"{matching_ns}.clinical_significance"] == TEST_CLINVAR_CONTROL["clinical_significance"]
    assert rows[0][f"{matching_ns}.clinical_review_status"] == TEST_CLINVAR_CONTROL["clinical_review_status"]
    # Non-matching version: all rows are NA.
    assert all(row[f"{non_matching_ns}.clinical_significance"] == "NA" for row in rows)
    assert all(row[f"{non_matching_ns}.clinical_review_status"] == "NA" for row in rows)


def test_invalid_clinvar_namespace_returns_422(client, setup_router_db, data_files):
    """A clinvar namespace with an out-of-range month (13) is rejected with 422."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/variants/data?namespaces=clinvar.2024_13")
    assert response.status_code == 422


def test_unrecognized_namespace_returns_422(client, setup_router_db, data_files):
    """An entirely unrecognized namespace string is rejected with 422."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/variants/data?namespaces=unknown_namespace")
    assert response.status_code == 422


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

    response = client.get(f"/api/v1/score-sets/{score_set['urn'] + 'xxx'}/clinical-controls")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn'] + 'xxx'}' not found" in response_data["detail"]


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


def test_clinical_control_options_exclude_non_current(client, setup_router_db, session, data_provider, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_clinical_controls_to_mapped_variants(session, score_set)

    # Mark all mapped variants as non-current to simulate stale mapping data.
    mapped_variants = session.scalars(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
    ).all()
    for mv in mapped_variants:
        mv.current = False
    session.commit()

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/clinical-controls/options")
    assert response.status_code == 404


########################################################################################################################
# Fetching annotated variants for a score set
########################################################################################################################


@pytest.mark.parametrize("annotation_type", ["pathogenicity-statement", "functional-statement", "study-result"])
def test_cannot_get_annotated_variants_for_nonexistent_score_set(client, setup_router_db, annotation_type):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn'] + 'xxx'}/annotated-variants/{annotation_type}")
    response_data = response.json()

    assert response.status_code == 404
    assert f"score set with URN {score_set['urn'] + 'xxx'} not found" in response_data["detail"]


@pytest.mark.parametrize("annotation_type", ["pathogenicity-statement", "functional-statement", "study-result"])
def test_cannot_get_annotated_variants_for_score_set_with_no_mapped_variants(
    client, session, data_provider, data_files, setup_router_db, annotation_type
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as queue:
        publish_score_set_response = client.post(f"/api/v1/score-sets/{score_set['urn']}/publish")
        assert publish_score_set_response.status_code == 200
        queue.assert_called_once()

    publish_score_set = publish_score_set_response.json()

    download_scores_csv_response = client.get(
        f"/api/v1/score-sets/{publish_score_set['urn']}/scores?drop_unused_hgvs_columns=true"
    )
    assert download_scores_csv_response.status_code == 200
    download_scores_csv = download_scores_csv_response.text
    csv_header = download_scores_csv.split("\n")[0]
    columns = csv_header.split(",")
    assert "hgvs_nt" in columns
    assert "hgvs_pro" in columns
    assert "hgvs_splice" not in columns

    response = client.get(f"/api/v1/score-sets/{publish_score_set['urn']}/annotated-variants/{annotation_type}")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"No mapped variants associated with score set URN {publish_score_set['urn']} were found"
        in response_data["detail"]
    )


# Tests that annotated variants of the correct type are returned when appropriate. The contents of these
# annotated variants are not tested here, and are tested in more detail via the annotation library tests.


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_annotated_pathogenicity_evidence_lines_for_score_set(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    # The contents of the annotated variants objects should be tested in more detail elsewhere.
    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/pathogenicity-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        variant_urn = annotation_response.get("variant_urn")
        annotated_variant = annotation_response.get("annotation")
        assert f"Variant pathogenicity statement for {variant_urn}" in annotated_variant.get("description", "")


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_nonetype_annotated_pathogenicity_evidence_lines_for_score_set_when_thresholds_not_present(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/pathogenicity-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant is None


def test_nonetype_annotated_pathogenicity_evidence_lines_for_score_set_when_calibrations_not_present(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/pathogenicity-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant is None


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_annotated_pathogenicity_evidence_lines_for_score_set_when_some_variants_were_not_mapped(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    first_var = clear_first_mapped_variant_post_mapped(session, score_set["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/pathogenicity-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        variant_urn = annotation_response.get("variant_urn")
        annotated_variant = annotation_response.get("annotation")
        if variant_urn == first_var.urn:
            assert annotated_variant is None
        else:
            assert f"Variant pathogenicity statement for {variant_urn}" in annotated_variant.get("description", "")


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_annotated_functional_impact_statement_for_score_set(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/functional-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant.get("type") == "Statement"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_nonetype_annotated_functional_impact_statement_for_score_set_when_calibrations_not_present(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize([TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, TEST_PATHOGENICITY_SCORE_CALIBRATION]),
        },
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/functional-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant is None


def test_nonetype_annotated_functional_impact_statement_for_score_set_when_thresholds_and_ranges_not_present(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/functional-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant is None


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_annotated_functional_impact_statement_for_score_set_when_some_variants_were_not_mapped(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    first_var = clear_first_mapped_variant_post_mapped(session, score_set["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/functional-statement")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        variant_urn = annotation_response.get("variant_urn")
        annotated_variant = annotation_response.get("annotation")
        if variant_urn == first_var.urn:
            assert annotated_variant is None
        else:
            assert annotated_variant.get("type") == "Statement"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_get_annotated_functional_study_result_for_score_set(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant.get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_annotated_functional_study_result_exists_for_score_set_when_thresholds_not_present(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize([TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, TEST_PATHOGENICITY_SCORE_CALIBRATION]),
        },
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant.get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_annotated_functional_study_result_exists_for_score_set_when_ranges_not_present(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize([TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, TEST_PATHOGENICITY_SCORE_CALIBRATION]),
        },
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant.get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


def test_annotated_functional_study_result_exists_for_score_set_when_thresholds_and_ranges_not_present(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        annotated_variant = annotation_response.get("annotation")
        assert annotated_variant.get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_annotated_functional_study_result_exists_for_score_set_when_some_variants_were_not_mapped(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize([TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, TEST_PATHOGENICITY_SCORE_CALIBRATION]),
        },
    )

    first_var = clear_first_mapped_variant_post_mapped(session, score_set["urn"])

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")
    response_data = parse_ndjson_response(response)

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for annotation_response in response_data:
        variant_urn = annotation_response.get("variant_urn")
        annotated_variant = annotation_response.get("annotation")
        if variant_urn == first_var.urn:
            assert annotated_variant is None
        else:
            assert annotated_variant.get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


def test_annotation_stream_reports_a_failing_variant_instead_of_truncating(
    client, session, data_provider, data_files, setup_router_db
):
    """One variant that cannot be annotated must not cost the consumer the rest of the download."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    failing_annotation = create_failing_side_effect(
        # Representative of lib/annotation/util.py, which raises this on an unrecognized VRS Allele state.
        ValueError("Unsupported VRS state type"),
        variant_study_result,
        fail_on_call=2,
    )

    with patch("mavedb.routers.score_sets.variant_study_result", failing_annotation):
        response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")

    assert response.status_code == 200

    response_data = parse_ndjson_response(response)
    assert len(response_data) == score_set["numVariants"]

    errored = [record for record in response_data if "error" in record]
    assert len(errored) == 1
    assert errored[0]["annotation"] is None
    assert errored[0]["error"] == {"type": "ValueError", "detail": "Unsupported VRS state type"}

    for record in response_data:
        if "error" not in record:
            assert record["annotation"].get("type") == "ExperimentalVariantFunctionalImpactStudyResult"


def test_annotation_stream_emits_one_record_per_variant_despite_a_failure(
    client, session, data_provider, data_files, setup_router_db
):
    """Every line is a variant record, so a body shorter than X-Total-Count is a truncated one."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    unmapped_variant = clear_first_mapped_variant_post_mapped(session, score_set["urn"])
    assert unmapped_variant is not None

    failing_annotation = create_failing_side_effect(
        ValueError("Unsupported VRS state type"), variant_study_result, fail_on_call=2
    )

    with patch("mavedb.routers.score_sets.variant_study_result", failing_annotation):
        response = client.get(f"/api/v1/score-sets/{score_set['urn']}/annotated-variants/study-result")

    total_count = int(response.headers["X-Total-Count"])
    assert total_count == score_set["numVariants"]

    response_data = parse_ndjson_response(response)
    assert len(response_data) == total_count
    assert all("variant_urn" in record for record in response_data)

    # A variant with no mapping data is an expected absence, distinguishable from the failure.
    errored = [record for record in response_data if "error" in record]
    unannotated = [record for record in response_data if record["annotation"] is None and "error" not in record]
    assert len(errored) == 1
    assert len(unannotated) == 1
    assert unannotated[0]["variant_urn"] == unmapped_variant.urn


########################################################################################################################
# Building individual annotation stream records
#
# Driven directly rather than over HTTP: these branches are about how a failure is classified, and
# reaching any one of them through the endpoint costs the whole app and a database.
########################################################################################################################


class _StubAnnotation:
    def model_dump(self, **kwargs):
        return {"type": "Stub"}


class _UndumpableAnnotation:
    def model_dump(self, **kwargs):
        raise ValueError("Extension.value is required")


def _annotation_raising(exception):
    """An annotation function that fails."""

    def annotate(_mapped_variant):
        raise exception

    return annotate


@pytest.fixture
def mock_mapped_variant_for_stream():
    return create_mock_mapped_variant(clingen_allele_id="CA123456")


def test_annotation_stream_record_serializes_a_successful_annotation(mock_mapped_variant_for_stream):
    record, outcome = _annotation_stream_record(mock_mapped_variant_for_stream, lambda mv: _StubAnnotation())

    assert outcome == "annotated"
    assert record == {"variant_urn": mock_mapped_variant_for_stream.variant.urn, "annotation": {"type": "Stub"}}


def test_annotation_stream_record_treats_a_null_annotation_as_unannotated(mock_mapped_variant_for_stream):
    """A variant the annotation layer declines to annotate is an expected outcome, not a failure."""
    record, outcome = _annotation_stream_record(mock_mapped_variant_for_stream, lambda mv: None)

    assert outcome == "unannotated"
    assert record == {"variant_urn": mock_mapped_variant_for_stream.variant.urn, "annotation": None}


def test_annotation_stream_record_treats_missing_mapping_data_as_unannotated(mock_mapped_variant_for_stream):
    # Preserved deliberately: a missing mapping is an expected absence, and reporting it as an error would
    # tell consumers a variant failed when nothing went wrong.
    record, outcome = _annotation_stream_record(
        mock_mapped_variant_for_stream, _annotation_raising(MappingDataDoesntExistException("no post-mapped allele"))
    )

    assert outcome == "unannotated"
    assert "error" not in record
    assert record["annotation"] is None


@pytest.mark.parametrize(
    "exception",
    [
        # lib/annotation/study_result.py, on absent or malformed score data.
        KeyError("score"),
        TypeError("'NoneType' object is not subscriptable"),
        # lib/annotation/util.py, on an unrecognized VRS Allele state type.
        ValueError("Unsupported VRS state type"),
        IndexError("list index out of range"),
    ],
)
def test_annotation_stream_record_reports_any_other_failure_as_an_error(mock_mapped_variant_for_stream, exception):
    record, outcome = _annotation_stream_record(mock_mapped_variant_for_stream, _annotation_raising(exception))

    assert outcome == "errored"
    assert record["variant_urn"] == mock_mapped_variant_for_stream.variant.urn
    assert record["annotation"] is None
    assert record["error"] == {"type": type(exception).__name__, "detail": str(exception)}


def test_annotation_stream_record_reports_a_serialization_failure_as_an_error(mock_mapped_variant_for_stream):
    """An emitted object that no longer dumps is the shape-dependent failure this stream must survive.

    Commit 5c155f4d fixed exactly this: a required field combined with `exclude_none` produced an object
    that built successfully and then failed on the way out.
    """
    record, outcome = _annotation_stream_record(mock_mapped_variant_for_stream, lambda mv: _UndumpableAnnotation())

    assert outcome == "errored"
    assert record["error"] == {"type": "ValueError", "detail": "Extension.value is required"}


########################################################################################################################
# Fetching gnomad variants for a score set
########################################################################################################################


def test_can_fetch_current_gnomad_variants_for_score_set(client, setup_router_db, session, data_provider, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_gnomad_variants_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/gnomad-variants")
    assert response.status_code == 200

    response_data = response.json()
    assert len(response_data) == 1
    for gnomad_variant in response_data:
        mapped_variants = gnomad_variant.pop("mappedVariants")
        assert len(mapped_variants) == 1
        gnomad_variant_items = sorted(gnomad_variant.items())
        assert gnomad_variant_items == sorted(TEST_SAVED_GNOMAD_VARIANT.items())


def test_can_fetch_current_gnomad_variants_for_score_set_with_version(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_gnomad_variants_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/gnomad-variants?version={TEST_GNOMAD_DATA_VERSION}")
    assert response.status_code == 200

    response_data = response.json()
    assert len(response_data) == 1
    for gnomad_variant in response_data:
        mapped_variants = gnomad_variant.pop("mappedVariants")
        assert len(mapped_variants) == 1
        gnomad_variant_items = sorted(gnomad_variant.items())
        assert gnomad_variant_items == sorted(TEST_SAVED_GNOMAD_VARIANT.items())


def test_cannot_fetch_current_gnomad_variants_for_score_set_with_nonexistent_version(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_gnomad_variants_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/gnomad-variants?version=nonexistent_version")
    assert response.status_code == 404

    response_data = response.json()
    assert "detail" in response_data
    assert (
        response_data["detail"]
        == f"No gnomad variants matching the provided filters associated with score set URN {score_set['urn']} were found"
    )


def test_cannot_fetch_gnomad_variants_for_nonexistent_score_set(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    link_gnomad_variants_to_mapped_variants(session, score_set)

    response = client.get(f"/api/v1/score-sets/{score_set['urn'] + 'xxx'}/gnomad-variants")

    assert response.status_code == 404
    response_data = response.json()
    assert f"score set with URN '{score_set['urn'] + 'xxx'}' not found" in response_data["detail"]


def test_cannot_fetch_gnomad_variants_for_score_set_when_none_exist(
    client, setup_router_db, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    response = client.get(f"/api/v1/score-sets/{score_set['urn']}/gnomad-variants")

    assert response.status_code == 404
    response_data = response.json()
    assert (
        f"No gnomad variants matching the provided filters associated with score set URN {score_set['urn']} were found"
        in response_data["detail"]
    )


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_publish_withholds_a_community_private_calibration_from_the_score_set_owner(
    session, data_provider, client, setup_router_db, data_files, mock_publication_fetch
):
    """Owning a score set does not entitle its owner to every calibration attached to it.

    A community calibration -- one contributed by someone who is not a contributor to the score set -- is
    readable only by its own creator while private. The owner-facing mutation endpoints returned the score
    set wholesale, so publishing handed the owner a calibration they cannot fetch directly.
    """
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    calibration_item = session.query(ScoreCalibrationDbModel).filter_by(urn=calibration["urn"]).one()
    calibration_item.investigator_provided = False
    session.commit()
    change_ownership(session, calibration["urn"], ScoreCalibrationDbModel)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    assert (published.get("scoreCalibrations") or []) == []


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
            {"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_publish_returns_the_owners_own_private_calibration(
    session, data_provider, client, setup_router_db, data_files, mock_publication_fetch
):
    """The filter withholds only what a calibration's own READ rule withholds."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = mock_worker_variant_insertion(client, session, data_provider, score_set, data_files / "scores.csv")
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])

    assert [c["urn"] for c in (published.get("scoreCalibrations") or [])] == [calibration["urn"]]
