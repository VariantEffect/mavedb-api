from typing import TYPE_CHECKING

import jsonschema

from mavedb.lib.mondo import get_generic_disease_term
from mavedb.lib.score_calibrations import create_score_calibration_in_score_set
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.user import User
from mavedb.view_models.score_calibration import ScoreCalibrationCreate, ScoreCalibrationWithScoreSetUrn
from tests.helpers.constants import TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session


def create_test_range_based_score_calibration_in_score_set(
    db: "Session", score_set_urn: str, user: User
) -> ScoreCalibration:
    calibration_create = ScoreCalibrationCreate(
        **TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, score_set_urn=score_set_urn
    )
    created_score_calibration = create_score_calibration_in_score_set(db, calibration_create, user)
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


def add_pathogenicity_calibration(db, score_set, variants_in_abnormal_range, urn, title, research_use_only=False):
    """Attach a calibration with a normal (BS3) and abnormal (PS3) range to *score_set*.

    Only *variants_in_abnormal_range* are associated with the abnormal range, which is what
    ``functional_classification_of_variant`` consults to classify a variant.
    """
    # A public calibration is only readable when its score set is too.
    score_set.private = False
    db.add(score_set)

    calibration = ScoreCalibration(
        score_set_id=score_set.id,
        urn=urn,
        title=title,
        baseline_score=0.0,
        research_use_only=research_use_only,
        primary=True,
        private=False,
        calibration_metadata={},
        disease_term=get_generic_disease_term(db),
        created_by_id=score_set.created_by_id,
        modified_by_id=score_set.modified_by_id,
    )
    db.add(calibration)
    db.commit()
    db.refresh(calibration)

    abnormal_acmg = db.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.PS3).first()
    normal_acmg = db.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.BS3).first()

    db.add(
        ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="test abnormal functional range",
            description="An abnormal functional range",
            functional_classification=FunctionalClassificationOptions.abnormal,
            range=[-5.0, -1.0],
            inclusive_lower_bound=True,
            inclusive_upper_bound=False,
            acmg_classification_id=abnormal_acmg.id,
            variants=list(variants_in_abnormal_range),
        )
    )
    db.add(
        ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="test normal functional range",
            description="A normal functional range",
            functional_classification=FunctionalClassificationOptions.normal,
            range=[1.0, 5.0],
            inclusive_lower_bound=True,
            inclusive_upper_bound=False,
            acmg_classification_id=normal_acmg.id,
            variants=[],
        )
    )
    db.commit()
    db.refresh(calibration)

    return calibration
