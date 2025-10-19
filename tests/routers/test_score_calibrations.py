# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from copy import deepcopy
from unittest.mock import patch

from arq import ArqRedis
from sqlalchemy import select

from mavedb.models.score_calibration import ScoreCalibration as CalibrationDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.contributor import add_contributor
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import (
    create_publish_and_promote_score_calibration,
    create_test_score_calibration_in_score_set_via_client,
    publish_test_score_calibration_via_client,
)
from tests.helpers.util.score_set import create_seq_score_set_with_mapped_variants, publish_score_set

from tests.helpers.constants import (
    EXTRA_USER,
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_PUBMED_IDENTIFIER,
    VALID_CALIBRATION_URN,
)

###########################################################
# GET /score-calibrations/{calibration_urn}
###########################################################


def test_cannot_get_score_calibration_when_not_exists(client, setup_router_db):
    response = client.get(f"/api/v1/score-calibrations/{VALID_CALIBRATION_URN}")

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


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
def test_anonymous_user_cannot_get_score_calibration_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_cannot_get_score_calibration_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_creating_user_can_get_score_calibration_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_contributing_user_can_get_score_calibration_when_private_and_investigator_provided(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = TEST_BRNICH_SCORE_CALIBRATION.copy()
    calibration_data["investigator_provided"] = True
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(calibration_data)
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_contributing_user_can_get_score_calibration_when_private_and_not_investigator_provided(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_admin_user_can_get_score_calibration_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_can_get_score_calibration_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    calibration = publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_can_get_score_calibration_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    calibration = publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_creating_user_can_get_score_calibration_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    calibration = publish_test_score_calibration_via_client(client, calibration["urn"])

    response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_contributing_user_can_get_score_calibration_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    calibration = publish_test_score_calibration_via_client(client, calibration["urn"])

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_admin_user_can_get_score_calibration_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    calibration = publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


###########################################################
#  GET /score-calibrations/score-set/{score_set_urn}
###########################################################


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_get_score_calibrations_for_score_set_when_none_exist(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert "No score calibrations found for the requested score set" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_cannot_get_score_calibrations_for_score_set_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_cannot_get_score_calibrations_for_score_set_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_cannot_get_score_calibrations_for_score_set_when_published_but_calibrations_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert "No score calibrations found for the requested score set" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_cannot_get_score_calibrations_for_score_set_when_published_but_calibrations_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert "No score calibrations found for the requested score set" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_creating_user_can_get_score_calibrations_for_score_set_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 1
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_contributing_user_can_get_investigator_provided_score_calibrations_for_score_set_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    calibration_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigator_provided"] = True
    investigator_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(calibration_data)
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 1
    assert calibrations_response[0]["urn"] == investigator_calibration["urn"]
    assert calibrations_response[0]["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_admin_user_can_get_score_calibrations_for_score_set_when_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 1
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_can_get_score_calibrations_for_score_set_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    # add another calibration that will remain private. The anonymous user should not see this one
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    publish_test_score_calibration_via_client(client, calibration["urn"])

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 1
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_can_get_score_calibrations_for_score_set_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    # add another calibration that will remain private. The other user should not see this one
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    publish_test_score_calibration_via_client(client, calibration["urn"])

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 1
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_cannot_get_score_calibrations_for_score_set_when_calibrations_public_score_set_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    # add another calibration that will remain private. The anonymous user should not see this one
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_other_user_cannot_get_score_calibrations_for_score_set_when_calibrations_public_score_set_private(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    # add another calibration that will remain private. The other user should not see this one
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_creating_user_can_get_score_calibrations_for_score_set_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # add another calibration that is private. The creating user should see this one too
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 2
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_contributing_user_can_get_score_calibrations_for_score_set_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # add another investigator provided calibration that is private. The contributing user should see this one too
    calibration_data = deepcamelize(deepcopy(TEST_BRNICH_SCORE_CALIBRATION))
    calibration_data["investigatorProvided"] = True
    create_test_score_calibration_in_score_set_via_client(client, score_set["urn"], calibration_data)

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 2
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_admin_user_can_get_score_calibrations_for_score_set_when_public(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # add another calibration that is private. The admin user should see this one too
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200
    calibrations_response = response.json()
    assert len(calibrations_response) == 2
    assert calibrations_response[0]["urn"] == calibration["urn"]
    assert calibrations_response[0]["private"] is False


###########################################################
# GET /score-calibrations/score-set/{score_set_urn}/primary
###########################################################


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_get_primary_score_calibration_for_score_set_when_no_calibrations_exist(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}/primary")

    assert response.status_code == 404
    error = response.json()
    assert "No primary score calibrations found for the requested score set" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_get_primary_score_calibration_for_score_set_when_none_exist(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}/primary")

    assert response.status_code == 404
    error = response.json()
    assert "No primary score calibrations found for the requested score set" in error["detail"]


# primary calibrations may not be private, so no need to test different user roles


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_primary_score_calibration_for_score_set_when_exists(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}/primary")

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["private"] is False


# TODO#544: Business logic on view models should prevent this case from arising in production, but it could occur if the database
# were sloppily edited directly.
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_get_primary_score_calibration_for_score_set_when_multiple_exist(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    create_publish_and_promote_score_calibration(client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION))
    calibration2 = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration2["urn"])

    second_primary = session.execute(
        select(CalibrationDbModel).where(CalibrationDbModel.urn == calibration2["urn"])
    ).scalar_one()
    second_primary.primary = True
    session.add(second_primary)
    session.commit()

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}/primary")

    assert response.status_code == 500
    error = response.json()
    assert "Multiple primary score calibrations found for the requested score set" in error["detail"]


###########################################################
# POST /score-calibrations
###########################################################


def test_cannot_create_score_calibration_when_missing_score_set_urn(client, setup_router_db):
    response = client.post(
        "/api/v1/score-calibrations",
        json={**deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)},
    )

    assert response.status_code == 422
    error = response.json()
    assert "score_set_urn must be provided to create a score calibration" in str(error["detail"])


def test_cannot_create_score_calibration_when_score_set_does_not_exist(client, setup_router_db):
    response = client.post(
        "/api/v1/score-calibrations",
        json={
            "scoreSetUrn": "urn:ngs:score-set:nonexistent",
            **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
        },
    )

    assert response.status_code == 404
    error = response.json()
    assert "score set with URN 'urn:ngs:score-set:nonexistent' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_calibration_when_score_set_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            "/api/v1/score-calibrations",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_calibration_in_public_score_set_when_score_set_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            "/api/v1/score-calibrations",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 403
    error = response.json()
    assert f"insufficient permissions for URN '{score_set['urn']}'" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_create_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            "/api/v1/score-calibrations",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_create_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.post(
        "/api/v1/score-calibrations",
        json={
            "scoreSetUrn": score_set["urn"],
            **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
        },
    )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_create_score_calibration_as_score_set_contributor(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            "/api/v1/score-calibrations",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_create_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.post(
            "/api/v1/score-calibrations",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["private"] is True


###########################################################
# PUT /score-calibrations/{calibration_urn}
###########################################################


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_calibration_when_score_set_not_exists(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json={
            "scoreSetUrn": "urn:ngs:score-set:nonexistent",
            **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
        },
    )

    assert response.status_code == 404
    error = response.json()
    assert "score set with URN 'urn:ngs:score-set:nonexistent' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_calibration_when_calibration_not_exists(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    response = client.put(
        "/api/v1/score-calibrations/urn:ngs:score-calibration:nonexistent",
        json={
            "scoreSetUrn": score_set["urn"],
            **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
        },
    )

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_calibration_when_score_set_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_score_calibration_in_published_score_set_when_score_set_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 403
    error = response.json()
    assert f"insufficient permissions for URN '{score_set['urn']}'" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_update_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json={
            "scoreSetUrn": score_set["urn"],
            **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
        },
    )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["name"] == TEST_PATHOGENICITY_SCORE_CALIBRATION["name"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_update_investigator_provided_score_calibration_as_score_set_contributor(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigatorProvided"] = True
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set["urn"], calibration_data)

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["name"] == TEST_PATHOGENICITY_SCORE_CALIBRATION["name"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_update_non_investigator_score_calibration_as_score_set_contributor(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 404
    calibration_response = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in calibration_response["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_update_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set["urn"],
                **deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["scoreSetUrn"] == score_set["urn"]
    assert calibration_response["name"] == TEST_PATHOGENICITY_SCORE_CALIBRATION["name"]
    assert calibration_response["private"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_anonymous_user_may_not_move_calibration_to_another_score_set(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set1 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    score_set2 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigatorProvided"] = False
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set1["urn"], calibration_data)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set2["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_user_may_not_move_investigator_calibration_when_lacking_permissions_on_destination_score_set(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set1 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    score_set2 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigatorProvided"] = True
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set1["urn"], calibration_data)

    # Give user permissions on the first score set only
    add_contributor(
        session,
        score_set1["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set2["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 404
    error = response.json()
    assert f"score set with URN '{score_set2['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_user_may_move_investigator_calibration_when_has_permissions_on_destination_score_set(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set1 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    score_set2 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigatorProvided"] = True
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set1["urn"], calibration_data)

    # Give user permissions on both score sets
    add_contributor(
        session,
        score_set1["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )
    add_contributor(
        session,
        score_set2["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with patch.object(ArqRedis, "enqueue_job", return_value=None):
        score_set1 = publish_score_set(client, score_set1["urn"])
        score_set2 = publish_score_set(client, score_set2["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set2["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["scoreSetUrn"] == score_set2["urn"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_admin_user_may_move_calibration_to_another_score_set(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set1 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    score_set2 = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set1["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json={
                "scoreSetUrn": score_set2["urn"],
                **deepcamelize(TEST_BRNICH_SCORE_CALIBRATION),
            },
        )

    assert response.status_code == 200
    calibration_response = response.json()
    assert calibration_response["urn"] == calibration["urn"]
    assert calibration_response["scoreSetUrn"] == score_set2["urn"]


###########################################################
# DELETE /score-calibrations/{calibration_urn}
###########################################################


def test_cannot_delete_score_calibration_when_not_exists(client, setup_router_db, session, data_provider, data_files):
    response = client.delete("/api/v1/score-calibrations/urn:ngs:score-calibration:nonexistent")

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_delete_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_delete_score_calibration_when_score_set_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 404
    error = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_delete_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 204

    # verify it's deleted
    get_response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")
    assert get_response.status_code == 404


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_delete_investigator_score_calibration_as_score_set_contributor(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration_data = deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    calibration_data["investigatorProvided"] = True
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set["urn"], calibration_data)

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 204

    # verify it's deleted
    get_response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")
    assert get_response.status_code == 404


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_delete_non_investigator_calibration_as_score_set_contributor(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    add_contributor(
        session,
        score_set["urn"],
        ScoreSetDbModel,
        EXTRA_USER["username"],
        EXTRA_USER["first_name"],
        EXTRA_USER["last_name"],
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_delete_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 204

    # verify it's deleted
    get_response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")
    assert get_response.status_code == 404


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_delete_primary_score_calibration(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.delete(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 403
    error = response.json()
    assert f"insufficient permissions for URN '{calibration['urn']}'" in error["detail"]


###########################################################
# POST /score-calibrations/{calibration_urn}/promote-to-primary
###########################################################


def test_cannot_promote_score_calibration_when_not_exists(client, setup_router_db, session, data_provider, data_files):
    response = client.post(
        "/api/v1/score-calibrations/urn:ngs:score-calibration:nonexistent/promote-to-primary",
        json={"calibrationUrn": "urn:ngs:score-calibration:nonexistent"},
    )

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_promote_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary")

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_promote_score_calibration_when_score_calibration_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary",
        )

    assert response.status_code == 403
    error = response.json()
    assert f"insufficient permissions for URN '{calibration['urn']}'" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_promote_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])
    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary")

    assert response.status_code == 200
    promotion_response = response.json()
    assert promotion_response["urn"] == calibration["urn"]
    assert promotion_response["scoreSetUrn"] == score_set["urn"]
    assert promotion_response["primary"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_promote_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(admin_app_overrides):
        response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary")

    assert response.status_code == 200
    promotion_response = response.json()
    assert promotion_response["urn"] == calibration["urn"]
    assert promotion_response["scoreSetUrn"] == score_set["urn"]
    assert promotion_response["primary"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_promote_existing_primary_to_primary(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    primary_calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.post(f"/api/v1/score-calibrations/{primary_calibration['urn']}/promote-to-primary")

    assert response.status_code == 200
    promotion_response = response.json()
    assert promotion_response["urn"] == primary_calibration["urn"]
    assert promotion_response["scoreSetUrn"] == score_set["urn"]
    assert promotion_response["primary"] is True


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_promote_research_use_only_to_primary(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client,
        score_set["urn"],
        deepcamelize({**TEST_BRNICH_SCORE_CALIBRATION, "researchUseOnly": True}),
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary")

    assert response.status_code == 400
    error = response.json()
    assert "Research use only score calibrations cannot be promoted to primary" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_promote_private_calibration_to_primary(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client,
        score_set["urn"],
        deepcamelize({**TEST_BRNICH_SCORE_CALIBRATION, "private": True}),
    )

    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/promote-to-primary")

    assert response.status_code == 400
    error = response.json()
    assert "Private score calibrations cannot be promoted to primary" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_promote_to_primary_if_primary_exists(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION))
    secondary_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, secondary_calibration["urn"])

    response = client.post(f"/api/v1/score-calibrations/{secondary_calibration['urn']}/promote-to-primary")

    assert response.status_code == 400
    error = response.json()
    assert "A primary score calibration already exists for this score set" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_promote_to_primary_if_primary_exists_when_demote_existing_is_true(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    primary_calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )
    secondary_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION)
    )
    publish_test_score_calibration_via_client(client, secondary_calibration["urn"])

    response = client.post(
        f"/api/v1/score-calibrations/{secondary_calibration['urn']}/promote-to-primary?demoteExistingPrimary=true",
    )

    assert response.status_code == 200
    promotion_response = response.json()
    assert promotion_response["urn"] == secondary_calibration["urn"]
    assert promotion_response["scoreSetUrn"] == score_set["urn"]
    assert promotion_response["primary"] is True

    # verify the previous primary is no longer primary
    get_response = client.get(f"/api/v1/score-calibrations/{primary_calibration['urn']}")
    assert get_response.status_code == 200
    previous_primary = get_response.json()
    assert previous_primary["primary"] is False


###########################################################
# POST /score-calibrations/{calibration_urn}/demote-from-primary
###########################################################


def test_cannot_demote_score_calibration_when_not_exists(client, setup_router_db):
    response = client.post(
        "/api/v1/score-calibrations/urn:ngs:score-calibration:nonexistent/demote-from-primary",
    )

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_demote_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/demote-from-primary",
        )

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_demote_score_calibration_when_score_calibration_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/demote-from-primary",
        )

    assert response.status_code == 403
    error = response.json()
    assert f"insufficient permissions for URN '{calibration['urn']}'" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_demote_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.post(
        f"/api/v1/score-calibrations/{calibration['urn']}/demote-from-primary",
    )

    assert response.status_code == 200
    demotion_response = response.json()
    assert demotion_response["urn"] == calibration["urn"]
    assert demotion_response["scoreSetUrn"] == score_set["urn"]
    assert demotion_response["primary"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_demote_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/demote-from-primary",
        )

    assert response.status_code == 200
    demotion_response = response.json()
    assert demotion_response["urn"] == calibration["urn"]
    assert demotion_response["scoreSetUrn"] == score_set["urn"]
    assert demotion_response["primary"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_demote_non_primary_score_calibration(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    create_publish_and_promote_score_calibration(client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION))
    secondary_calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_PATHOGENICITY_SCORE_CALIBRATION)
    )

    response = client.post(
        f"/api/v1/score-calibrations/{secondary_calibration['urn']}/demote-from-primary",
    )

    assert response.status_code == 200
    demotion_response = response.json()
    assert demotion_response["urn"] == secondary_calibration["urn"]
    assert demotion_response["scoreSetUrn"] == score_set["urn"]
    assert demotion_response["primary"] is False


###########################################################
# POST /score-calibrations/{calibration_urn}/publish
###########################################################


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_publish_score_calibration_when_not_exists(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    response = client.post(
        "/api/v1/score-calibrations/urn:ngs:score-calibration:nonexistent/publish",
    )

    assert response.status_code == 404
    error = response.json()
    assert "The requested score calibration does not exist" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_publish_score_calibration_as_anonymous_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(anonymous_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/publish",
        )

    assert response.status_code == 401
    error = response.json()
    assert "Could not validate credentials" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_cannot_publish_score_calibration_when_score_calibration_not_owned_by_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, extra_user_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(extra_user_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/publish",
        )

    assert response.status_code == 404
    error = response.json()
    assert f"score calibration with URN '{calibration['urn']}' not found" in error["detail"]


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_publish_score_calibration_as_score_set_owner(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    response = client.post(
        f"/api/v1/score-calibrations/{calibration['urn']}/publish",
    )

    assert response.status_code == 200
    publish_response = response.json()
    assert publish_response["urn"] == calibration["urn"]
    assert publish_response["scoreSetUrn"] == score_set["urn"]
    assert publish_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_publish_score_calibration_as_admin_user(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.post(
            f"/api/v1/score-calibrations/{calibration['urn']}/publish",
        )

    assert response.status_code == 200
    publish_response = response.json()
    assert publish_response["urn"] == calibration["urn"]
    assert publish_response["scoreSetUrn"] == score_set["urn"]
    assert publish_response["private"] is False


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        [
            {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
            {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
        ]
    ],
    indirect=["mock_publication_fetch"],
)
def test_can_publish_already_published_calibration(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION)
    )

    # publish it first
    publish_response_1 = client.post(
        f"/api/v1/score-calibrations/{calibration['urn']}/publish",
    )
    assert publish_response_1.status_code == 200
    published_calibration_1 = publish_response_1.json()
    assert published_calibration_1["private"] is False

    # publish it again
    publish_response_2 = client.post(
        f"/api/v1/score-calibrations/{calibration['urn']}/publish",
    )
    assert publish_response_2.status_code == 200
    published_calibration_2 = publish_response_2.json()
    assert published_calibration_2["private"] is False
