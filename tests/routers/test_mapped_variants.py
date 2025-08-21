# ruff: noqa: E402

import pytest
import json

from tests.helpers.util.user import change_ownership

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from humps import camelize
from sqlalchemy import select
from sqlalchemy.orm.session import make_transient
from urllib.parse import quote_plus

from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.view_models.mapped_variant import SavedMappedVariant

from tests.helpers.constants import (
    TEST_PUBMED_IDENTIFIER,
    TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED,
    TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
    TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT,
)
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    create_seq_score_set_with_mapped_variants,
    create_seq_score_set_with_variants,
)


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_show_mapped_variant(client, session, data_provider, data_files, setup_router_db, mock_publication_fetch):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["id"] == 1

    SavedMappedVariant.model_validate_json(json.dumps(response_data))


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_show_mapped_variant_study_result(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant effect study result for {score_set['urn']}#1."

    ExperimentalVariantFunctionalImpactStudyResult.model_validate_json(json.dumps(response_data))


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_study_result_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_study_result_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_study_result_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a study result for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_show_mapped_variant_functional_impact_statement(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant functional impact statement for {score_set['urn']}#1."

    Statement.model_validate_json(json.dumps(response_data))


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_functional_impact_statement_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_functional_impact_statement_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_functional_impact_statement_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a functional impact statement for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_functional_impact_statement_when_no_score_ranges(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-impact")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a functional impact statement for mapped variant {score_set['urn']}#1: No score range evidence found"
        in response_data["detail"]
    )


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_show_mapped_variant_clinical_evidence_line(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#2')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Pathogenicity evidence line {score_set['urn']}#2."

    VariantPathogenicityEvidenceLine.model_validate_json(json.dumps(response_data))


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_clinical_evidence_line_when_multiple_exist(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    session.expunge(item)
    make_transient(item)
    item.id = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 500
    assert response_data["detail"] == f"Multiple variants with URN {score_set['urn']}#1 were found."


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_clinical_evidence_line_when_none_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert response_data["detail"] == f"Mapped variant with URN {score_set['urn']}#1 not found"


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_clinical_evidence_line_when_no_mapping_data_exists(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT),
        },
    )

    item = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert item is not None

    item.post_mapped = None
    session.add(item)
    session.commit()

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a pathogenicity evidence line for mapped variant {score_set['urn']}#1: Variant {score_set['urn']}#1 does not have a post mapped variant."
        in response_data["detail"]
    )


@pytest.mark.parametrize(
    "mock_publication_fetch",
    [({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"})],
    indirect=["mock_publication_fetch"],
)
def test_cannot_show_mapped_variant_clinical_evidence_line_when_no_score_calibrations_exist(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
        update={
            "secondaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}],
            "scoreRanges": camelize(TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED),
        },
    )

    response = client.get(f"/api/v1/mapped-variants/{quote_plus(score_set['urn'] + '#1')}/va/clinical-evidence")
    response_data = response.json()

    assert response.status_code == 404
    assert (
        f"Could not construct a pathogenicity evidence line for mapped variant {score_set['urn']}#1; No calibrations exist"
        in response_data["detail"]
    )


def test_show_mapped_variants_by_ga4gh_identifier(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    mapped_variant = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert mapped_variant is not None

    response = client.get(f"/api/v1/mapped-variants/vrs/{quote_plus(mapped_variant.pre_mapped['id'])}")
    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for response_data in response_data:
        assert response_data["preMapped"]["id"] == mapped_variant.pre_mapped["id"]
        SavedMappedVariant.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variants_by_ga4gh_identifier_when_none_exist(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    mapped_variant = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert mapped_variant is not None

    fake_mapped_variant_id = mapped_variant.pre_mapped["id"]
    fake_mapped_variant_id = fake_mapped_variant_id[:-3] + "aaa"  # Modify the ID to ensure it doesn't exist

    response = client.get(f"/api/v1/mapped-variants/vrs/{quote_plus(fake_mapped_variant_id)}")
    assert response.status_code == 404


def test_show_mapped_variants_by_ga4gh_identifier_with_non_current_variants(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    mapped_variant = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert mapped_variant is not None

    # Set the mapped variant to non-current
    mapped_variant.current = False
    session.add(mapped_variant)
    session.commit()

    response = client.get(
        f"/api/v1/mapped-variants/vrs/{quote_plus(mapped_variant.pre_mapped['id'])}?only_current=false"
    )
    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"]

    for response_data in response_data:
        assert response_data["preMapped"]["id"] == mapped_variant.pre_mapped["id"]
        SavedMappedVariant.model_validate_json(json.dumps(response_data))


def test_show_mapped_variants_by_ga4gh_identifier_with_only_current_variants(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    mapped_variant = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    mapped_variant2 = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#2'))
    assert mapped_variant is not None
    assert mapped_variant2 is not None

    # Set the mapped variant to non-current
    mapped_variant2.current = False
    mapped_variant2.pre_mapped = mapped_variant.pre_mapped  # Ensure both pre mapped blobs match besides current status
    session.add(mapped_variant)
    session.commit()

    response = client.get(
        f"/api/v1/mapped-variants/vrs/{quote_plus(mapped_variant.pre_mapped['id'])}?only_current=true"
    )
    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == score_set["numVariants"] - 1

    for response_data in response_data:
        assert response_data["preMapped"]["id"] == mapped_variant.pre_mapped["id"]
        SavedMappedVariant.model_validate_json(json.dumps(response_data))


def test_cannot_show_mapped_variants_by_ga4gh_identifier_without_score_set_permissions(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores.csv",
    )

    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    mapped_variant = session.scalar(select(MappedVariant).join(Variant).where(Variant.urn == f'{score_set["urn"]}#1'))
    assert mapped_variant is not None

    response = client.get(f"/api/v1/mapped-variants/vrs/{quote_plus(mapped_variant.pre_mapped['id'])}")
    assert response.status_code == 404
