# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from sqlalchemy import select

from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.score_calibration import ScoreCalibration as CalibrationDbModel
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_PUBMED_IDENTIFIER,
)
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import create_test_score_calibration_in_score_set_via_client
from tests.helpers.util.score_set import create_seq_score_set_with_mapped_variants

CALIBRATION_PUBLICATIONS = [
    {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
    {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
]


def _create_private_calibration(client, session, data_provider, data_files):
    """Create a score set with variants and a private range-based calibration; return the calibration dict."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )
    return create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )


def _attach_control(session, calibration_urn, controls_not_phi):
    """Attach one control to a calibration at the ORM level (endpoint wiring is #753) and set the PHI flag."""
    calibration = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration_urn).one()
    variant = session.scalars(select(Variant).where(Variant.score_set_id == calibration.score_set_id)).first()

    session.add(
        CalibrationControl(
            calibration=calibration,
            variant=variant,
            clinical_status=CalibrationControlStatus.pathogenic,
            created_by=calibration.created_by,
            modified_by=calibration.created_by,
        )
    )
    calibration.controls_not_phi = controls_not_phi
    session.add(calibration)
    session.commit()


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
@pytest.mark.parametrize("controls_not_phi", [None, False])
def test_cannot_publish_calibration_with_controls_when_phi_not_affirmed(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, controls_not_phi
):
    calibration = _create_private_calibration(client, session, data_provider, data_files)
    _attach_control(session, calibration["urn"], controls_not_phi=controls_not_phi)

    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/publish")

    assert response.status_code == 422
    assert "protected health information" in response.json()["detail"].lower()

    # The gate must not have published the calibration.
    refreshed = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()
    assert refreshed.private is True


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_can_publish_calibration_with_controls_when_phi_affirmed(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    calibration = _create_private_calibration(client, session, data_provider, data_files)
    _attach_control(session, calibration["urn"], controls_not_phi=True)

    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/publish")

    assert response.status_code == 200
    assert response.json()["private"] is False


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_can_publish_calibration_without_controls_regardless_of_phi_flag(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    calibration = _create_private_calibration(client, session, data_provider, data_files)

    # No controls attached, but an explicit (non-True) PHI flag must not block publishing.
    item = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()
    item.controls_not_phi = False
    session.add(item)
    session.commit()

    response = client.post(f"/api/v1/score-calibrations/{calibration['urn']}/publish")

    assert response.status_code == 200
    assert response.json()["private"] is False
