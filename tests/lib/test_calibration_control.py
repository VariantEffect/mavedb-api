# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy.exc import IntegrityError

from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER


def _make_calibration(session, score_set_id: int, user: User) -> ScoreCalibration:
    """Persist a bare calibration on the given score set; controls need a parent calibration."""
    calibration = ScoreCalibration(
        title="Calibration with controls",
        score_set_id=score_set_id,
        created_by=user,
        modified_by=user,
    )
    session.add(calibration)
    session.commit()
    session.refresh(calibration)
    return calibration


def test_calibration_control_persists_and_links(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    calibration = _make_calibration(session, variant.score_set_id, user)

    control = CalibrationControl(
        calibration=calibration,
        variant=variant,
        clinical_status=CalibrationControlStatus.pathogenic,
        created_by=user,
        modified_by=user,
    )
    session.add(control)
    session.commit()
    session.refresh(control)

    assert control.id is not None
    # Enum value round-trips through the varchar column.
    assert control.clinical_status is CalibrationControlStatus.pathogenic
    assert control.creation_date is not None
    # Relationship resolves both directions via back_populates.
    assert control.calibration is calibration
    assert control.variant is variant
    assert control in calibration.controls


def test_calibration_control_unique_per_calibration_and_variant(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    calibration = _make_calibration(session, variant.score_set_id, user)

    session.add(
        CalibrationControl(
            calibration=calibration,
            variant=variant,
            clinical_status=CalibrationControlStatus.pathogenic,
            created_by=user,
            modified_by=user,
        )
    )
    session.commit()

    # A second control for the same (calibration, variant) pair violates the unique constraint,
    # even with a different clinical status — a variant is one piece of evidence per calibration.
    session.add(
        CalibrationControl(
            calibration=calibration,
            variant=variant,
            clinical_status=CalibrationControlStatus.benign,
            created_by=user,
            modified_by=user,
        )
    )
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_deleting_calibration_cascades_to_controls(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    calibration = _make_calibration(session, variant.score_set_id, user)

    control = CalibrationControl(
        calibration=calibration,
        variant=variant,
        clinical_status=CalibrationControlStatus.benign,
        created_by=user,
        modified_by=user,
    )
    session.add(control)
    session.commit()
    control_id = control.id

    # A control has no meaning apart from its calibration, so deleting the calibration removes it.
    session.delete(calibration)
    session.commit()

    assert session.get(CalibrationControl, control_id) is None
