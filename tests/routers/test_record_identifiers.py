# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from copy import deepcopy

from sqlalchemy import select

from mavedb.models.doi_identifier import ExperimentDoiIdentifier, ScoreSetDoiIdentifier
from mavedb.models.raw_read_identifier import ExperimentRawReadIdentifier
from mavedb.models.target_gene_external_identifier import TargetGeneExternalIdentifier
from tests.helpers.constants import TEST_MINIMAL_EXPERIMENT, TEST_MINIMAL_SEQ_SCORESET
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import create_seq_score_set


def _target_genes_with(external_identifiers):
    target_genes = deepcopy(TEST_MINIMAL_SEQ_SCORESET["targetGenes"])
    target_genes[0]["externalIdentifiers"] = external_identifiers
    return target_genes


ENSEMBL = {"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}
UNIPROT = {"identifier": {"dbName": "UniProt", "identifier": "Q9Y617"}, "offset": 3}
REFSEQ = {"identifier": {"dbName": "RefSeq", "identifier": "NP_478059.1"}, "offset": 6}


def test_create_experiment_with_identifiers_stores_one_upper_cased_doi_and_typed_raw_read_urls(
    session, client, setup_router_db
):
    experiment = create_experiment(
        client,
        {
            "doiIdentifiers": [{"identifier": "10.7554/eLife.58026"}, {"identifier": "10.7554/ELIFE.58026"}],
            "rawReadIdentifiers": [
                {"identifier": "SRP002725"},
                {"identifier": "PRJNA941815"},
                {"identifier": "GSE226732"},
                {"identifier": "E-MTAB-1234"},
            ],
        },
    )

    assert experiment["doiIdentifiers"] == [
        {
            "recordType": "DoiIdentifier",
            "identifier": "10.7554/ELIFE.58026",
            "url": "https://doi.org/10.7554/ELIFE.58026",
        }
    ]
    assert {identifier["identifier"]: identifier["url"] for identifier in experiment["rawReadIdentifiers"]} == {
        "SRP002725": "https://www.ebi.ac.uk/ena/browser/view/SRP002725",
        "PRJNA941815": "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA941815",
        "GSE226732": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE226732",
        "E-MTAB-1234": "https://www.ebi.ac.uk/biostudies/arrayexpress/studies/E-MTAB-1234",
    }
    assert session.scalars(select(ExperimentDoiIdentifier.identifier)).all() == ["10.7554/ELIFE.58026"]


def test_update_experiment_identifiers_keeps_resubmitted_and_drops_omitted(session, client, setup_router_db):
    experiment = create_experiment(
        client,
        {
            "doiIdentifiers": [{"identifier": "10.1000/182"}],
            "rawReadIdentifiers": [{"identifier": "SRP002725"}],
        },
    )

    payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    payload.update(
        {
            "doiIdentifiers": [{"identifier": "10.1000/182"}, {"identifier": "10.1000/183"}],
            "rawReadIdentifiers": [],
        }
    )
    response = client.put(f"/api/v1/experiments/{experiment['urn']}", json=payload)

    assert response.status_code == 200
    assert [identifier["identifier"] for identifier in response.json()["doiIdentifiers"]] == [
        "10.1000/182",
        "10.1000/183",
    ]
    assert response.json()["rawReadIdentifiers"] == []
    assert session.scalars(select(ExperimentRawReadIdentifier)).all() == []


def test_create_score_set_with_doi_stores_it_on_the_score_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], {"doiIdentifiers": [{"identifier": "10.1000/abc"}]})

    assert score_set["doiIdentifiers"] == [
        {"recordType": "DoiIdentifier", "identifier": "10.1000/ABC", "url": "https://doi.org/10.1000/ABC"}
    ]
    assert session.scalars(select(ScoreSetDoiIdentifier.identifier)).all() == ["10.1000/ABC"]


def test_create_score_set_with_external_identifiers_returns_them_nested_with_urls(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(
        client, experiment["urn"], {"targetGenes": _target_genes_with([UNIPROT, ENSEMBL, REFSEQ])}
    )

    assert score_set["targetGenes"][0]["externalIdentifiers"] == [
        {
            "recordType": "ExternalGeneIdentifierOffset",
            "offset": 0,
            "identifier": {
                "recordType": "ExternalGeneIdentifier",
                "dbName": "Ensembl",
                "identifier": "ENSG00000103275",
                "url": "https://www.ensembl.org/id/ENSG00000103275",
            },
        },
        {
            "recordType": "ExternalGeneIdentifierOffset",
            "offset": 6,
            "identifier": {
                "recordType": "ExternalGeneIdentifier",
                "dbName": "RefSeq",
                "identifier": "NP_478059.1",
                "url": "https://www.ncbi.nlm.nih.gov/protein/NP_478059.1",
            },
        },
        {
            "recordType": "ExternalGeneIdentifierOffset",
            "offset": 3,
            "identifier": {
                "recordType": "ExternalGeneIdentifier",
                "dbName": "UniProt",
                "identifier": "Q9Y617",
                "url": "https://www.uniprot.org/uniprotkb/Q9Y617",
            },
        },
    ]


def test_update_score_set_external_identifiers_replaces_the_target_genes_set(session, client, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], {"targetGenes": _target_genes_with([UNIPROT, ENSEMBL])})

    payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    payload.update(
        {
            "experimentUrn": experiment["urn"],
            "targetGenes": _target_genes_with([{**UNIPROT, "offset": 9}]),
        }
    )
    response = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=payload)

    assert response.status_code == 200
    rows = session.execute(select(TargetGeneExternalIdentifier.db_name, TargetGeneExternalIdentifier.offset)).all()
    assert rows == [("UniProt", 9)]


@pytest.mark.parametrize(
    "route, text, identifier, make_record",
    [
        (
            "/api/v1/doi-identifiers/search",
            "10.1000",
            "10.1000/182",
            lambda client: create_experiment(client, {"doiIdentifiers": [{"identifier": "10.1000/182"}]}),
        ),
        (
            "/api/v1/raw-read-identifiers/search",
            "SRP",
            "SRP002725",
            lambda client: create_experiment(client, {"rawReadIdentifiers": [{"identifier": "SRP002725"}]}),
        ),
        (
            "/api/v1/target-gene-identifiers/search?db_name=UniProt",
            "Q9Y",
            "Q9Y617",
            lambda client: create_seq_score_set(
                client, create_experiment(client)["urn"], {"targetGenes": _target_genes_with([UNIPROT])}
            ),
        ),
    ],
    ids=["doi", "raw-read", "target-gene"],
)
def test_search_identifiers_on_private_record_visible_only_to_those_who_can_read_it(
    client, setup_router_db, anonymous_app_overrides, extra_user_app_overrides, route, text, identifier, make_record
):
    make_record(client)

    owner_response = client.post(route, json={"text": text})
    with DependencyOverrider(anonymous_app_overrides):
        anonymous_response = client.post(route, json={"text": text})
    with DependencyOverrider(extra_user_app_overrides):
        other_user_response = client.post(route, json={"text": text})

    assert owner_response.status_code == 200
    assert [result["identifier"] for result in owner_response.json()] == [identifier]
    assert anonymous_response.json() == []
    assert other_user_response.json() == []


def test_search_doi_identifiers_cited_twice_returns_one_result(client, setup_router_db):
    experiment = create_experiment(client, {"doiIdentifiers": [{"identifier": "10.1000/182"}]})
    create_seq_score_set(client, experiment["urn"], {"doiIdentifiers": [{"identifier": "10.1000/182"}]})

    response = client.post("/api/v1/doi-identifiers/search", json={"text": "10.1000"})

    assert response.status_code == 200
    assert response.json() == [
        {"recordType": "DoiIdentifier", "identifier": "10.1000/182", "url": "https://doi.org/10.1000/182"}
    ]


@pytest.mark.parametrize(
    "route",
    [
        "/api/v1/doi-identifiers/search",
        "/api/v1/raw-read-identifiers/search",
        "/api/v1/target-gene-identifiers/search?db_name=UniProt",
    ],
    ids=["doi", "raw-read", "target-gene"],
)
def test_search_identifiers_without_text_returns_400(client, setup_router_db, route):
    response = client.post(route, json={"text": " "})

    assert response.status_code == 400


def test_search_target_gene_identifiers_unknown_database_returns_422(client, setup_router_db):
    response = client.post("/api/v1/target-gene-identifiers/search?db_name=GenBank", json={"text": "Q9Y"})

    assert response.status_code == 422
