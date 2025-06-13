# ruff: noqa: E402

import pytest
import json

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from humps import camelize
from sqlalchemy import select
from sqlalchemy.orm.session import make_transient
from urllib.parse import quote_plus

from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityFunctionalImpactEvidenceLine
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.view_models.mapped_variant import SavedMappedVariant

from tests.helpers.constants import TEST_SCORE_CALIBRATION, TEST_SCORE_SET_RANGE
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    add_thresholds_to_score_set,
    create_seq_score_set_with_mapped_variants,
    create_seq_score_set_with_variants,
)


def test_show_mapped_variant(client, session, data_provider, data_files, setup_router_db, admin_app_overrides):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["id"] == 1

    SavedMappedVariant.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variant_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    print([mv.variant.urn for mv in session.scalars(select(MappedVariant)).all()])

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


def test_cannot_show_mapped_variant_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


def test_show_mapped_variant_study_result(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant effect study result for {score_set['urn']}#1."

    ExperimentalVariantFunctionalImpactStudyResult.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variant_study_result_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


def test_cannot_show_mapped_variant_study_result_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


def test_cannot_show_mapped_variant_study_result_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a study result for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


def test_show_mapped_variant_functional_impact_statement(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant functional impact statement for {score_set['urn']}#1."

    Statement.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variant_functional_impact_statement_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


def test_cannot_show_mapped_variant_functional_impact_statement_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


def test_cannot_show_mapped_variant_functional_impact_statement_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a functional impact statement for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


def test_cannot_show_mapped_variant_functional_impact_statement_when_no_score_ranges(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": None},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a functional impact statement for mapped variant {score_set['urn']}#1: No score range evidence found"
        in response_data["detail"]
    )


def test_show_mapped_variant_clinical_evidence_line(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#2')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Pathogenicity evidence line {score_set['urn']}#2."

    VariantPathogenicityFunctionalImpactEvidenceLine.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variant_clinical_evidence_line_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


def test_cannot_show_mapped_variant_clinical_evidence_line_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


def test_cannot_show_mapped_variant_clinical_evidence_line_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, admin_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a pathogenicity evidence line for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


def test_cannot_show_mapped_variant_clinical_evidence_line_when_no_score_calibrations_exist(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a pathogenicity evidence line for mapped variant {score_set['urn']}#1; No calibrations exist"
        in response_data["detail"]
    )
