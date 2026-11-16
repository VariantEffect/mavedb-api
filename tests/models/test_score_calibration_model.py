# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from mavedb.lib.mondo import get_generic_disease_term
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER


@pytest.fixture
def version_chain(session, setup_lib_db_with_score_set):
    """A published calibration and the published version that replaces it."""
    user = session.scalars(select(User).where(User.username == TEST_USER["username"])).one()
    disease_term = get_generic_disease_term(session)

    def calibration(title, **kwargs):
        return ScoreCalibration(
            title=title,
            private=False,
            score_set=setup_lib_db_with_score_set,
            disease_term_id=disease_term.id,
            created_by=user,
            modified_by=user,
            **kwargs,
        )

    original = calibration("Original")
    replacement = calibration("Replacement", superseded_calibration=original)
    session.add_all([original, replacement])
    session.commit()

    return original, replacement


def test_score_calibration_replacing_itself_is_rejected(session, version_chain):
    original, _ = version_chain
    original.superseded_calibration_id = original.id

    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def test_score_calibration_deleting_a_replaced_version_is_rejected(session, version_chain):
    original, replacement = version_chain
    session.delete(original)

    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()

    session.refresh(replacement)
    assert replacement.superseded_calibration_id == original.id


def test_score_calibration_deleting_the_score_set_removes_every_version(session, version_chain):
    original, _ = version_chain
    session.delete(original.score_set)
    session.commit()

    assert session.scalars(select(ScoreCalibration)).all() == []
