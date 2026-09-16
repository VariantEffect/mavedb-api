from copy import deepcopy
from datetime import date

import pytest
from pydantic import ValidationError

from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.view_models.calibration_control import (
    CalibrationControlCreate,
    SavedCalibrationControl,
)
from mavedb.view_models.score_calibration import ScoreCalibration, ScoreCalibrationCreate
from tests.helpers.constants import (
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_USER,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict

TEST_CONTROL_VARIANT_URN = "urn:mavedb:00000001-a-1#1"


def _saved_user_like():
    """An attributed object standing in for a persisted User (audit field)."""
    return dummy_attributed_object_from_dict({"username": TEST_USER["username"]})


def _control_attributed_object(status=CalibrationControlStatus.pathogenic):
    """An attributed object standing in for a persisted CalibrationControl ORM row."""
    return dummy_attributed_object_from_dict(
        {
            "id": 1,
            "variant": dummy_attributed_object_from_dict({"urn": TEST_CONTROL_VARIANT_URN}),
            "clinical_status": status,
            "creation_date": date(2026, 9, 16),
            "modification_date": date(2026, 9, 16),
            "created_by": _saved_user_like(),
            "modified_by": _saved_user_like(),
        }
    )


##############################################################################
# CalibrationControl view models from dicts (request bodies)
##############################################################################


def test_calibration_control_create_from_dict():
    control = CalibrationControlCreate(**{"variantUrn": TEST_CONTROL_VARIANT_URN, "clinicalStatus": "pathogenic"})

    assert control.variant_urn == TEST_CONTROL_VARIANT_URN
    assert control.clinical_status is CalibrationControlStatus.pathogenic
    assert control.model_dump(by_alias=True)["variantUrn"] == TEST_CONTROL_VARIANT_URN


@pytest.mark.parametrize("status", ["pathogenic", "benign"])
def test_calibration_control_create_accepts_both_statuses(status):
    control = CalibrationControlCreate(variant_urn=TEST_CONTROL_VARIANT_URN, clinical_status=status)
    assert control.clinical_status is CalibrationControlStatus(status)


def test_calibration_control_create_rejects_unknown_status():
    with pytest.raises(ValidationError):
        CalibrationControlCreate(variant_urn=TEST_CONTROL_VARIANT_URN, clinical_status="likely_pathogenic")


def test_calibration_control_create_requires_variant_urn():
    with pytest.raises(ValidationError):
        CalibrationControlCreate(clinical_status="benign")


##############################################################################
# SavedCalibrationControl from attributed objects (ORM models)
##############################################################################


def test_saved_calibration_control_synthesizes_variant_urn_from_orm():
    saved = SavedCalibrationControl.model_validate(_control_attributed_object())

    # variant_urn is not a column on the ORM row; it is derived from the variant relationship.
    assert saved.variant_urn == TEST_CONTROL_VARIANT_URN
    assert saved.clinical_status is CalibrationControlStatus.pathogenic
    assert saved.id == 1
    assert saved.created_by.username == TEST_USER["username"]
    assert saved.modified_by.username == TEST_USER["username"]
    assert saved.record_type == "SavedCalibrationControl"

    dumped = saved.model_dump(by_alias=True)
    assert dumped["variantUrn"] == TEST_CONTROL_VARIANT_URN
    assert dumped["recordType"] == "SavedCalibrationControl"


##############################################################################
# ScoreCalibration view models extended with controls / disease / controls_not_phi
##############################################################################


def test_score_calibration_create_accepts_controls_disease_and_phi():
    payload = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    payload["disease"] = "Brugada syndrome"
    payload["controls_not_phi"] = True
    payload["controls"] = [
        {"variant_urn": TEST_CONTROL_VARIANT_URN, "clinical_status": "pathogenic"},
        {"variant_urn": "urn:mavedb:00000001-a-1#2", "clinical_status": "benign"},
    ]

    calibration = ScoreCalibrationCreate.model_validate(payload)

    assert calibration.disease == "Brugada syndrome"
    assert calibration.controls_not_phi is True
    assert len(calibration.controls) == 2
    assert calibration.controls[0].clinical_status is CalibrationControlStatus.pathogenic
    assert calibration.controls[1].clinical_status is CalibrationControlStatus.benign


def test_saved_score_calibration_includes_controls():
    saved = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    saved["disease"] = "Brugada syndrome"
    saved["controlsNotPhi"] = True
    saved["controls"] = [
        {
            "id": 1,
            "variantUrn": TEST_CONTROL_VARIANT_URN,
            "clinicalStatus": "pathogenic",
            "creationDate": date(2026, 9, 16),
            "modificationDate": date(2026, 9, 16),
            "createdBy": {"orcidId": TEST_USER["username"]},
            "modifiedBy": {"orcidId": TEST_USER["username"]},
        }
    ]

    calibration = ScoreCalibration.model_validate(dummy_attributed_object_from_dict(saved))

    assert calibration.disease == "Brugada syndrome"
    assert calibration.controls_not_phi is True
    assert len(calibration.controls) == 1
    assert calibration.controls[0].variant_urn == TEST_CONTROL_VARIANT_URN
    assert calibration.controls[0].clinical_status is CalibrationControlStatus.pathogenic


def test_saved_score_calibration_controls_default_to_empty_list():
    saved = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    saved.pop("controls", None)

    calibration = ScoreCalibration.model_validate(dummy_attributed_object_from_dict(saved))

    assert calibration.controls == []
