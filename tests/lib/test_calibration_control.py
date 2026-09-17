# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

import pandas as pd
from sqlalchemy.exc import IntegrityError

from mavedb.lib.score_calibrations import build_calibration_controls, validate_calibration_controls_in_score_set
from mavedb.lib.validation.constants.general import (
    calibration_control_status_column_name,
    calibration_variant_column_name,
    hgvs_nt_column,
    hgvs_pro_column,
)
from mavedb.lib.validation.dataframe.calibration import validate_and_standardize_calibration_controls_dataframe
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.lib.mondo import get_generic_disease_term
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.calibration_control import CalibrationControlCreate
from tests.helpers.constants import TEST_LICENSE, TEST_MINIMAL_VARIANT, TEST_SEQ_SCORESET, TEST_USER


def _make_calibration(session, score_set_id: int, user: User) -> ScoreCalibration:
    """Persist a bare calibration on the given score set; controls need a parent calibration."""
    calibration = ScoreCalibration(
        title="Calibration with controls",
        score_set_id=score_set_id,
        disease_term=get_generic_disease_term(session),
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


##############################################################################
# validate_calibration_controls_in_score_set (#751)
##############################################################################


def _variant_in_other_score_set(session, reference_score_set, user) -> Variant:
    """Create a second score set (in the same experiment) with one variant, for cross-set tests."""
    scaffold = TEST_SEQ_SCORESET.copy()
    scaffold.pop("target_genes")
    other_score_set = ScoreSet(
        **scaffold,
        urn="urn:mavedb:00000002-a-1",
        experiment_id=reference_score_set.experiment_id,
        licence_id=TEST_LICENSE["id"],
    )
    other_score_set.created_by = user
    other_score_set.modified_by = user
    session.add(other_score_set)
    session.commit()
    session.refresh(other_score_set)

    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{other_score_set.urn}#1", score_set_id=other_score_set.id)
    session.add(variant)
    session.commit()
    session.refresh(variant)
    return variant


@pytest.mark.parametrize("controls", [None, []])
def test_validate_controls_is_a_no_op_when_absent(session, setup_lib_db_with_variant, controls):
    variant = setup_lib_db_with_variant
    # Must not raise for None or an empty list, and returns no URNs.
    assert validate_calibration_controls_in_score_set(session, variant.score_set, controls) == []


def test_validate_controls_accepts_variant_in_score_set(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    controls = [CalibrationControlCreate(variant_urn=variant.urn, clinical_status="pathogenic")]

    # Returns the validated URNs so persistence can reuse them without rebuilding the list.
    assert validate_calibration_controls_in_score_set(session, variant.score_set, controls) == [variant.urn]


def test_validate_controls_rejects_nonexistent_variant(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    controls = [CalibrationControlCreate(variant_urn="urn:mavedb:99999999-x-9#1", clinical_status="benign")]

    with pytest.raises(ValidationError, match="do not belong to the calibration's score set"):
        validate_calibration_controls_in_score_set(session, variant.score_set, controls)


def test_validate_controls_rejects_variant_from_another_score_set(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    other_variant = _variant_in_other_score_set(session, variant.score_set, user)

    controls = [CalibrationControlCreate(variant_urn=other_variant.urn, clinical_status="pathogenic")]

    with pytest.raises(ValidationError, match="do not belong to the calibration's score set"):
        validate_calibration_controls_in_score_set(session, variant.score_set, controls)


def test_validate_controls_rejects_duplicate_variant_urns(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    controls = [
        CalibrationControlCreate(variant_urn=variant.urn, clinical_status="pathogenic"),
        CalibrationControlCreate(variant_urn=variant.urn, clinical_status="benign"),
    ]

    with pytest.raises(ValidationError, match="Duplicate control variant URNs detected"):
        validate_calibration_controls_in_score_set(session, variant.score_set, controls)


##############################################################################
# build_calibration_controls (#753 lib persistence)
##############################################################################


@pytest.mark.parametrize("controls", [None, []])
def test_build_calibration_controls_empty_for_absent_input(session, setup_lib_db_with_variant, controls):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()

    assert build_calibration_controls(session, variant.score_set, controls, user) == []


def test_build_calibration_controls_constructs_rows(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    controls = [CalibrationControlCreate(variant_urn=variant.urn, clinical_status="benign")]

    built = build_calibration_controls(session, variant.score_set, controls, user)

    assert len(built) == 1
    assert built[0].variant is variant
    assert built[0].clinical_status is CalibrationControlStatus.benign
    assert built[0].created_by is user
    assert built[0].modified_by is user


def test_build_calibration_controls_propagates_validation_error(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()
    controls = [CalibrationControlCreate(variant_urn="urn:mavedb:99999999-x-9#1", clinical_status="benign")]

    with pytest.raises(ValidationError, match="do not belong to the calibration's score set"):
        build_calibration_controls(session, variant.score_set, controls, user)


##############################################################################
# validate_and_standardize_calibration_controls_dataframe (#753 CSV path)
##############################################################################


def _controls_df(variant_values, statuses):
    return pd.DataFrame(
        {calibration_variant_column_name: variant_values, calibration_control_status_column_name: statuses}
    )


def test_controls_csv_validates_and_converts_to_create_rows(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    # Status is case-insensitive.
    df = _controls_df([variant.urn], ["Pathogenic"])

    controls = validate_and_standardize_calibration_controls_dataframe(session, variant.score_set, df)

    assert len(controls) == 1
    assert controls[0].variant_urn == variant.urn
    assert controls[0].clinical_status is CalibrationControlStatus.pathogenic


def test_controls_csv_rejects_invalid_status(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    df = _controls_df([variant.urn], ["likely_pathogenic"])

    with pytest.raises(ValidationError, match="Invalid clinical status"):
        validate_and_standardize_calibration_controls_dataframe(session, variant.score_set, df)


def test_controls_csv_rejects_variant_not_in_score_set(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    df = _controls_df(["urn:mavedb:99999999-x-9#1"], ["benign"])

    with pytest.raises(ValidationError, match="do not exist in the score set"):
        validate_and_standardize_calibration_controls_dataframe(session, variant.score_set, df)


def test_controls_csv_rejects_missing_status_column(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    df = pd.DataFrame({calibration_variant_column_name: [variant.urn]})

    with pytest.raises(ValidationError, match=calibration_control_status_column_name):
        validate_and_standardize_calibration_controls_dataframe(session, variant.score_set, df)


def test_controls_csv_accepts_hgvs_nt_index(session, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    # hgvs_nt is unique within the score set, so it resolves 1:1 to the variant URN.
    df = pd.DataFrame({hgvs_nt_column: [variant.hgvs_nt], calibration_control_status_column_name: ["benign"]})

    controls = validate_and_standardize_calibration_controls_dataframe(session, variant.score_set, df)

    assert len(controls) == 1
    assert controls[0].variant_urn == variant.urn
    assert controls[0].clinical_status is CalibrationControlStatus.benign


@pytest.mark.parametrize(
    "index_column, shared_value, distinct_values",
    [
        # hgvs_pro collision: two nucleotide variants collapsing to the same protein consequence
        # (the expected case in a nucleotide-indexed score set).
        (hgvs_pro_column, "p.Met1Val", ("c.1A>G", "c.3G>T")),
        # hgvs_nt collision: not expected in practice, but the detection must still fire.
        (hgvs_nt_column, "c.99A>G", ("p.Lys33Arg", "p.Lys33Gln")),
    ],
)
def test_controls_csv_rejects_ambiguous_hgvs_index(
    session, setup_lib_db_with_variant, index_column, shared_value, distinct_values
):
    variant = setup_lib_db_with_variant
    score_set = variant.score_set

    # Two variants that share the submitted index value but differ on the other HGVS field, so the
    # index resolves to more than one variant in the score set.
    for suffix, distinct_value in enumerate(distinct_values, start=2):
        hgvs_nt = shared_value if index_column == hgvs_nt_column else distinct_value
        hgvs_pro = shared_value if index_column == hgvs_pro_column else distinct_value
        session.add(
            Variant(
                data=TEST_MINIMAL_VARIANT["data"],
                hgvs_nt=hgvs_nt,
                hgvs_pro=hgvs_pro,
                creation_date=TEST_MINIMAL_VARIANT["creation_date"],
                modification_date=TEST_MINIMAL_VARIANT["modification_date"],
                urn=f"{score_set.urn}#{suffix}",
                score_set_id=score_set.id,
            )
        )
    session.commit()

    df = pd.DataFrame({index_column: [shared_value], calibration_control_status_column_name: ["pathogenic"]})

    with pytest.raises(ValidationError, match="match more than one variant"):
        validate_and_standardize_calibration_controls_dataframe(session, score_set, df)
