from typing import TYPE_CHECKING

import jsonschema

from mavedb.lib.score_calibrations import create_score_calibration_in_score_set
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.user import User
from mavedb.view_models.score_calibration import ScoreCalibrationCreate, ScoreCalibrationWithScoreSetUrn
from tests.helpers.constants import TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session


async def create_test_range_based_score_calibration_in_score_set(
    db: "Session", score_set_urn: str, user: User
) -> ScoreCalibration:
    calibration_create = ScoreCalibrationCreate(
        **TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, score_set_urn=score_set_urn
    )
    created_score_calibration = await create_score_calibration_in_score_set(db, calibration_create, user)
    assert created_score_calibration is not None

    db.commit()
    db.refresh(created_score_calibration)

    return created_score_calibration


def create_test_score_calibration_in_score_set_via_client(
    client: "TestClient", score_set_urn: str, calibration_data: dict
):
    calibration_payload = {**calibration_data, "scoreSetUrn": score_set_urn}
    jsonschema.validate(instance=calibration_payload, schema=ScoreCalibrationCreate.model_json_schema())

    response = client.post(
        "/api/v1/score-calibrations/",
        json=calibration_payload,
    )

    assert response.status_code == 200, "Could not create score calibration"

    calibration = response.json()
    assert calibration["scoreSetUrn"] == score_set_urn

    jsonschema.validate(instance=calibration, schema=ScoreCalibrationWithScoreSetUrn.model_json_schema())
    return calibration


def force_publish_test_score_calibration(db: "Session", calibration_urn: str) -> None:
    """Mark a calibration public without the publish route's checks.

    Reproduces calibrations that were published while their score set was still private, which the publish
    route now refuses.
    """
    calibration = db.query(ScoreCalibration).filter(ScoreCalibration.urn == calibration_urn).one()
    calibration.private = False
    db.add(calibration)
    db.commit()


def publish_test_score_calibration_via_client(client: "TestClient", calibration_urn: str):
    response = client.post(f"/api/v1/score-calibrations/{calibration_urn}/publish")

    assert response.status_code == 200, "Could not publish score calibration"

    calibration = response.json()
    assert calibration["private"] is False

    jsonschema.validate(instance=calibration, schema=ScoreCalibrationWithScoreSetUrn.model_json_schema())
    return calibration


def promote_test_score_calibration_to_primary_via_client(
    client: "TestClient", calibration_urn: str, demote_existing_primary: bool = False
):
    response = client.post(
        f"/api/v1/score-calibrations/{calibration_urn}/promote-to-primary",
        params={"demoteExistingPrimary": demote_existing_primary},
    )

    assert response.status_code == 200, "Could not promote score calibration to primary"

    calibration = response.json()
    assert calibration["primary"] is True

    jsonschema.validate(instance=calibration, schema=ScoreCalibrationWithScoreSetUrn.model_json_schema())
    return calibration


def create_publish_and_promote_score_calibration(client, score_set_urn: str, calibration_data: dict):
    calibration = create_test_score_calibration_in_score_set_via_client(client, score_set_urn, calibration_data)
    publish_test_score_calibration_via_client(client, calibration["urn"])
    promote_test_score_calibration_to_primary_via_client(client, calibration["urn"])
    return calibration
