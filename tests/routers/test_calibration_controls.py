# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

import json

from sqlalchemy import delete, select

from mavedb.lib.validation.constants.general import (
    calibration_control_status_column_name,
    calibration_variant_column_name,
)
from mavedb.models.calibration_control import CalibrationControl
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.models.score_calibration import ScoreCalibration as CalibrationDbModel
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_PUBMED_IDENTIFIER,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import (
    create_test_score_calibration_in_score_set_via_client,
    publish_test_score_calibration_via_client,
)
from tests.helpers.util.score_set import create_seq_score_set_with_mapped_variants

CALIBRATION_PUBLICATIONS = [
    {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
    {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
]


def _score_set_with_variant_urns(client, session, data_provider, data_files):
    """Create a score set with mapped variants; return (score_set dict, list of its variant URNs)."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    score_set_orm = session.query(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"]).one()
    variant_urns = [
        variant.urn
        for variant in session.scalars(select(Variant).where(Variant.score_set_id == score_set_orm.id)).all()
    ]
    return score_set, variant_urns


def _calibration_payload(score_set_urn, **extra):
    return {**deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED), "scoreSetUrn": score_set_urn, **extra}


CONTROLS_CSV_HEADER = f"{calibration_variant_column_name},{calibration_control_status_column_name}"


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


###########################################################
# Controls create / update / read wiring (#753)
###########################################################


def _create_with_controls(client, score_set_urn, controls, **extra):
    response = client.post(
        "/api/v1/score-calibrations/", json=_calibration_payload(score_set_urn, controls=controls, **extra)
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_create_calibration_with_inline_controls(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)

    calibration = _create_with_controls(
        client,
        score_set["urn"],
        [
            {"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"},
            {"variantUrn": variant_urns[1], "clinicalStatus": "benign"},
        ],
    )

    assert {(c["variantUrn"], c["clinicalStatus"]) for c in calibration["controls"]} == {
        (variant_urns[0], "pathogenic"),
        (variant_urns[1], "benign"),
    }


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_create_calibration_with_controls_file(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    csv = f"{CONTROLS_CSV_HEADER}\n{variant_urns[0]},pathogenic\n{variant_urns[1]},Benign\n"

    response = client.post(
        "/api/v1/score-calibrations/",
        data={"calibration_json": json.dumps(_calibration_payload(score_set["urn"]))},
        files={"controls_file": ("controls.csv", csv, "text/csv")},
    )

    assert response.status_code == 200, response.text
    assert {(c["variantUrn"], c["clinicalStatus"]) for c in response.json()["controls"]} == {
        (variant_urns[0], "pathogenic"),
        (variant_urns[1], "benign"),
    }


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_create_calibration_rejects_inline_controls_and_controls_file_together(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    csv = f"{CONTROLS_CSV_HEADER}\n{variant_urns[0]},pathogenic\n"

    response = client.post(
        "/api/v1/score-calibrations/",
        data={
            "calibration_json": json.dumps(
                _calibration_payload(
                    score_set["urn"], controls=[{"variantUrn": variant_urns[0], "clinicalStatus": "benign"}]
                )
            )
        },
        files={"controls_file": ("controls.csv", csv, "text/csv")},
    )

    assert response.status_code == 422
    assert "not both" in str(response.json()["detail"])


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_get_calibration_returns_controls(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client, score_set["urn"], [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
    )

    response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")

    assert response.status_code == 200
    assert response.json()["controls"][0]["variantUrn"] == variant_urns[0]


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_list_endpoint_reports_controls_count_without_full_controls(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    _create_with_controls(
        client,
        score_set["urn"],
        [
            {"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"},
            {"variantUrn": variant_urns[1], "clinicalStatus": "benign"},
        ],
    )

    response = client.get(f"/api/v1/score-calibrations/score-set/{score_set['urn']}")

    assert response.status_code == 200, response.text
    item = response.json()[0]
    # List responses carry the count, not the full controls list.
    assert item["controlsCount"] == 2
    assert "controls" not in item


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_modify_replaces_controls(client, setup_router_db, mock_publication_fetch, session, data_provider, data_files):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client, score_set["urn"], [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
    )

    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json=_calibration_payload(
            score_set["urn"], controls=[{"variantUrn": variant_urns[1], "clinicalStatus": "benign"}]
        ),
    )

    assert response.status_code == 200, response.text
    controls = response.json()["controls"]
    assert len(controls) == 1
    assert controls[0]["variantUrn"] == variant_urns[1]
    assert controls[0]["clinicalStatus"] == "benign"


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_modify_with_empty_controls_clears_them(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client, score_set["urn"], [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
    )

    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json=_calibration_payload(score_set["urn"], controls=[]),
    )

    assert response.status_code == 200, response.text
    assert response.json()["controls"] == []


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_modify_without_controls_leaves_them_unchanged(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client, score_set["urn"], [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
    )

    # The payload omits controls entirely, so existing controls must be preserved.
    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}", json=_calibration_payload(score_set["urn"])
    )

    assert response.status_code == 200, response.text
    assert len(response.json()["controls"]) == 1


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_modify_controls_without_affirmation_resets_phi_flag(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client,
        score_set["urn"],
        [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
        controlsNotPhi=True,
    )
    assert calibration["controlsNotPhi"] is True

    # Changing controls without re-affirming invalidates the prior affirmation.
    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json=_calibration_payload(
            score_set["urn"], controls=[{"variantUrn": variant_urns[1], "clinicalStatus": "benign"}]
        ),
    )

    assert response.status_code == 200, response.text
    assert response.json()["controlsNotPhi"] is None


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_modify_controls_with_affirmation_keeps_phi_flag(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client, score_set["urn"], [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
    )

    # Affirming in the same request that changes controls keeps the flag set.
    response = client.put(
        f"/api/v1/score-calibrations/{calibration['urn']}",
        json=_calibration_payload(
            score_set["urn"],
            controls=[{"variantUrn": variant_urns[1], "clinicalStatus": "benign"}],
            controlsNotPhi=True,
        ),
    )

    assert response.status_code == 200, response.text
    assert response.json()["controlsNotPhi"] is True


###########################################################
# PHI re-affirmation invariant on published calibrations
###########################################################


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_create_calibration_ignores_client_supplied_private_field(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    # The create schema has no `private` field — creation always starts private, and publishing
    # (which independently gates PHI-bearing controls) is the only path to making one public.
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)

    response = client.post(
        "/api/v1/score-calibrations/",
        json={
            **_calibration_payload(score_set["urn"]),
            "private": False,
            "controls": [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["private"] is True


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_cannot_add_controls_to_published_calibration_without_phi_affirmation_as_admin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json=_calibration_payload(
                score_set["urn"], controls=[{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}]
            ),
        )

    assert response.status_code == 422
    assert "protected health information" in response.json()["detail"].lower()

    # The gate must not have attached the controls.
    refreshed = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()
    assert refreshed.controls == []


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_can_add_controls_to_published_calibration_with_phi_affirmation_as_admin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = create_test_score_calibration_in_score_set_via_client(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json=_calibration_payload(
                score_set["urn"],
                controls=[{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
                controlsNotPhi=True,
            ),
        )

    assert response.status_code == 200, response.text
    assert response.json()["controlsNotPhi"] is True
    assert len(response.json()["controls"]) == 1


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_cannot_replace_controls_on_published_calibration_without_reaffirming_as_admin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client,
        score_set["urn"],
        [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
        controlsNotPhi=True,
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # Replacing controls invalidates the prior affirmation just as it does on a private calibration,
    # so an admin who doesn't re-affirm in the same request must be rejected here too.
    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json=_calibration_payload(
                score_set["urn"], controls=[{"variantUrn": variant_urns[1], "clinicalStatus": "benign"}]
            ),
        )

    assert response.status_code == 422
    assert "protected health information" in response.json()["detail"].lower()

    # The gate must not have replaced the controls or reset the affirmation.
    refreshed = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()
    assert refreshed.controls_not_phi is True
    assert len(refreshed.controls) == 1


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_cannot_decline_phi_affirmation_on_published_calibration_with_existing_controls_as_admin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client,
        score_set["urn"],
        [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
        controlsNotPhi=True,
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # Controls are left unchanged; only the affirmation is walked back to False.
    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json=_calibration_payload(score_set["urn"], controlsNotPhi=False),
        )

    assert response.status_code == 422
    assert "protected health information" in response.json()["detail"].lower()


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_can_update_published_calibration_leaving_affirmed_controls_untouched_as_admin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files, admin_app_overrides
):
    score_set, variant_urns = _score_set_with_variant_urns(client, session, data_provider, data_files)
    calibration = _create_with_controls(
        client,
        score_set["urn"],
        [{"variantUrn": variant_urns[0], "clinicalStatus": "pathogenic"}],
        controlsNotPhi=True,
    )
    publish_test_score_calibration_via_client(client, calibration["urn"])

    # An edit that neither submits controls nor touches controlsNotPhi must leave the standing
    # affirmation alone, so it should not trip the gate.
    with DependencyOverrider(admin_app_overrides):
        response = client.put(
            f"/api/v1/score-calibrations/{calibration['urn']}",
            json=_calibration_payload(score_set["urn"], notes="Updated notes"),
        )

    assert response.status_code == 200, response.text
    assert response.json()["controlsNotPhi"] is True
    assert len(response.json()["controls"]) == 1


###########################################################
# Control placement (functional_classification_id) — Option A
###########################################################


def _add_control_on_variant(session, calibration, variant, status=CalibrationControlStatus.pathogenic):
    """Attach one control to ``calibration`` on a chosen ``variant`` at the ORM level."""
    session.add(
        CalibrationControl(
            calibration=calibration,
            variant=variant,
            clinical_status=status,
            created_by=calibration.created_by,
            modified_by=calibration.created_by,
        )
    )
    session.commit()


def _placement_by_variant_id(calibration):
    """Ground-truth ``{variant_id: functional_classification_id}`` from stored bin membership."""
    placement = {}
    for classification in calibration.functional_classifications:
        for variant in classification.variants:
            placement[variant.id] = classification.id
    return placement


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_get_calibration_populates_control_placement(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    """A control whose variant a range files reports that range's id as its placement."""
    calibration = _create_private_calibration(client, session, data_provider, data_files)
    calibration_orm = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()

    placement = _placement_by_variant_id(calibration_orm)
    assert placement, "fixture calibration should file at least one variant into a range"

    variant_id, expected_classification_id = next(iter(placement.items()))
    variant = session.get(Variant, variant_id)
    _add_control_on_variant(session, calibration_orm, variant)

    response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")
    assert response.status_code == 200, response.text

    control = next(c for c in response.json()["controls"] if c["variantUrn"] == variant.urn)
    assert control["functionalClassificationId"] == expected_classification_id


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_control_placement_is_none_when_variant_has_no_bin(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    """A control whose variant is filed under none of the calibration's ranges reports null placement."""
    calibration = _create_private_calibration(client, session, data_provider, data_files)
    calibration_orm = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()

    variant = session.scalars(select(Variant).where(Variant.score_set_id == calibration_orm.score_set_id)).first()
    _add_control_on_variant(session, calibration_orm, variant)

    # Strip any bin membership for this variant so its placement must resolve to None.
    association = score_calibration_functional_classification_variants_association_table
    session.execute(delete(association).where(association.c.variant_id == variant.id))
    session.commit()

    response = client.get(f"/api/v1/score-calibrations/{calibration['urn']}")
    assert response.status_code == 200, response.text

    control = next(c for c in response.json()["controls"] if c["variantUrn"] == variant.urn)
    assert control["functionalClassificationId"] is None


@pytest.mark.parametrize("mock_publication_fetch", [CALIBRATION_PUBLICATIONS], indirect=["mock_publication_fetch"])
def test_control_placement_scoped_to_own_calibration(
    client, setup_router_db, mock_publication_fetch, session, data_provider, data_files
):
    """Placement reflects the control's own calibration, not another that happens to bin the variant."""
    calibration = _create_private_calibration(client, session, data_provider, data_files)
    calibration_orm = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == calibration["urn"]).one()

    # Capture plain ids/urns up front; creating the second calibration commits and expires ORM objects.
    score_set_urn = calibration_orm.score_set.urn
    placement = _placement_by_variant_id(calibration_orm)
    assert placement
    binned_variant_id = next(iter(placement))
    binned_variant_urn = session.get(Variant, binned_variant_id).urn

    # A second calibration on the same score set bins the same variant identically; clear only the
    # second's membership for it, so the first calibration's binning must not leak into the second's
    # placement.
    second = create_test_score_calibration_in_score_set_via_client(
        client, score_set_urn, deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    second_orm = session.query(CalibrationDbModel).where(CalibrationDbModel.urn == second["urn"]).one()

    association = score_calibration_functional_classification_variants_association_table
    second_classification_ids = [fc.id for fc in second_orm.functional_classifications]
    session.execute(
        delete(association).where(
            association.c.variant_id == binned_variant_id,
            association.c.functional_classification_id.in_(second_classification_ids),
        )
    )
    _add_control_on_variant(session, second_orm, session.get(Variant, binned_variant_id))

    response = client.get(f"/api/v1/score-calibrations/{second['urn']}")
    assert response.status_code == 200, response.text

    control = next(c for c in response.json()["controls"] if c["variantUrn"] == binned_variant_urn)
    assert control["functionalClassificationId"] is None
