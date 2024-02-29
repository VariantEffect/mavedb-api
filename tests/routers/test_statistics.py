import pytest
from unittest.mock import patch
from humps import camelize
import cdot.hgvs.dataproviders
from tests.helpers.util import (
    create_experiment,
    create_seq_score_set_with_variants,
    create_acc_score_set_with_variants,
    publish_score_set,
)
from tests.helpers.constants import (
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_PUBMED_IDENTIFIER,
    TEST_BIORXIV_IDENTIFIER,
    TEST_MEDRXIV_IDENTIFIER,
    TEST_CDOT_TRANSCRIPT,
)

TARGET_ACCESSION_FIELDS = ["accession", "assembly", "gene"]
TARGET_SEQUENCE_FIELDS = ["sequence", "sequence-type"]
TARGET_GENE_FIELDS = ["category", "organism", "reference"]
TARGET_GENE_IDENTIFIER_FIELDS = ["ensembl-identifier", "refseq-identifier", "uniprot-identifier"]

RECORD_MODELS = ["experiment", "score-set"]
RECORD_SHARED_FIELDS = ["publication-identifiers", "keywords", "doi-identifiers", "raw-read-identifiers", "created-by"]


# Fixtures for setting up score sets on which to calculate statistics.
@pytest.fixture()
def setup_acc_scoreset(setup_router_db, client, data_files):
    experiment = create_experiment(client)
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        score_set = create_acc_score_set_with_variants(client, experiment["urn"], data_files / "scores_acc.csv")
    publish_score_set(client, score_set["urn"])


@pytest.fixture()
def setup_seq_scoreset(setup_router_db, client, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    publish_score_set(client, score_set["urn"])


## Test base case empty database responses for each statistic endpoint.


def test_empty_database_statistics(client):
    stats_endpoints = (
        "target/accession/accession",
        "target/accession/assembly",
        "target/accession/gene",
        "target/sequence/sequence",
        "target/sequence/sequence-type",
        "target/gene/category",
        "target/gene/organism",
        "target/gene/reference",
        "target/gene/ensembl-identifier",
        "target/gene/uniprot-identifier",
        "target/gene/refseq-identifier",
        "record/experiment/publication-identifiers",
        "record/experiment/keywords",
        "record/experiment/doi-identifiers",
        "record/experiment/raw-read-identifiers",
        "record/experiment/created-by",
        "record/score-set/publication-identifiers",
        "record/score-set/keywords",
        "record/score-set/doi-identifiers",
        "record/score-set/raw-read-identifiers",
        "record/score-set/created-by",
    )
    for endpoint in stats_endpoints:
        response = client.get(f"/api/v1/statistics/{endpoint}")
        assert response.status_code == 200, f"Non-200 status code for endpoint {endpoint}."
        assert response.json() == {}, f"Non-empty response for endpoint {endpoint}."


## Test target accession statistics
@pytest.mark.parametrize(
    "field_value",
    TARGET_ACCESSION_FIELDS,
)
def test_target_accession_statistics(client, field_value, setup_acc_scoreset):
    """Test target accession statistics endpoint for published score sets."""
    camelized_field_value = camelize(field_value.replace("-", "_"))
    response = client.get(f"/api/v1/statistics/target/accession/{field_value}")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value]] == 1


def test_target_accession_invalid_field(client):
    """Test target accession statistic response for an invalid target accession field."""
    response = client.get("/api/v1/statistics/target/accession/invalid-field")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "field"]
    assert response.json()["detail"][0]["ctx"]["enum_values"] == TARGET_ACCESSION_FIELDS


def test_target_accession_empty_field(client):
    """Test target accession statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/accession/")
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


## Test target sequence statistics
@pytest.mark.parametrize(
    "field_value",
    TARGET_SEQUENCE_FIELDS,
)
def test_target_sequence_statistics(client, field_value, setup_seq_scoreset):
    """Test target sequence statistics endpoint for published score sets."""
    camelized_field_value = camelize(field_value.replace("-", "_"))
    response = client.get(f"/api/v1/statistics/target/sequence/{field_value}")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value]] == 1


def test_target_sequence_invalid_field(client):
    """Test target sequence statistic response for an invalid field."""
    response = client.get("/api/v1/statistics/target/sequence/invalid-field")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "field"]
    assert response.json()["detail"][0]["ctx"]["enum_values"] == TARGET_SEQUENCE_FIELDS


def test_target_sequence_empty_field(client):
    """Test target sequence statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/sequence/")
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


## Test target gene statistics.


# NOTE: Statistics on target genes behave differently depending on if the
def test_target_gene_category_statistics_acc(client, setup_acc_scoreset):
    """Test target gene category endpoint on accession based target for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/category")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]] == 1


def test_target_gene_category_statistics_seq(client, setup_seq_scoreset):
    """Test target gene category endpoint on sequence based target for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/category")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]] == 1


def test_target_gene_organism_statistics_acc(client, setup_acc_scoreset):
    """Test target gene organism endpoint on accession based targets for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/organism")

    assert "Homo sapiens" in response.json()
    assert response.json()["Homo sapiens"] == 1


def test_target_gene_organism_statistics_seq(client, setup_seq_scoreset):
    """Test target gene organism statistics on sequence based targets for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/organism")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"] in response.json()
    assert (
        response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"]] == 1
    )


def test_target_gene_reference_statistics_acc(client, setup_acc_scoreset):
    """Test target gene reference statistics on accession based targets for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/reference")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"]] == 1


def test_target_gene_reference_statistics_seq(client, setup_seq_scoreset):
    """Test target gene reference statistics on sequence based targets for published score sets."""
    response = client.get("/api/v1/statistics/target/gene/reference")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"]] == 1


# Target gene identifier based statistics behave similarly enough to parametrize.
@pytest.mark.parametrize("field_value", TARGET_GENE_IDENTIFIER_FIELDS)
def test_target_gene_identifier_statistiscs(client, setup_acc_scoreset, setup_seq_scoreset, data_files, field_value):
    """Test target gene identifier statistics endpoint for published score sets."""
    # Helper map that simplifies parametrization.
    test_mapper = {
        "ensembl-identifier": {"offset": 0, "identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}},
        "refseq-identifier": {"offset": 0, "identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}},
        "uniprot-identifier": {"offset": 0, "identifier": {"dbName": "UniProt", "identifier": "Q9Y617"}},
    }
    record_update = {"externalIdentifiers": [v for v in test_mapper.values()]}
    experiment = create_experiment(client)

    # Update each of the score set target genes to include our test external identifiers.
    seq_target = TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0].copy()
    seq_target.update(record_update)
    seq_score_set = create_seq_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", {"targetGenes": [seq_target]}
    )

    acc_target = TEST_MINIMAL_ACC_SCORESET["targetGenes"][0].copy()
    acc_target.update(record_update)
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_CDOT_TRANSCRIPT):
        acc_score_set = create_acc_score_set_with_variants(
            client, experiment["urn"], data_files / "scores_acc.csv", {"targetGenes": [acc_target]}
        )

    publish_score_set(client, seq_score_set["urn"])
    publish_score_set(client, acc_score_set["urn"])
    response = client.get(f"/api/v1/statistics/target/gene/{field_value}")

    assert response.status_code == 200
    assert test_mapper[field_value]["identifier"]["identifier"] in response.json()
    assert response.json()[test_mapper[field_value]["identifier"]["identifier"]] == 2


def test_target_gene_invalid_field(client):
    """Test target gene statistic response for an invalid field."""
    response = client.get("/api/v1/statistics/target/gene/invalid-field")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "field"]
    assert response.json()["detail"][0]["ctx"]["enum_values"] == TARGET_GENE_FIELDS + TARGET_GENE_IDENTIFIER_FIELDS


def test_target_gene_empty_field(client):
    """Test target gene statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/gene/")
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


## Test Experiment and Score Set statistics


@pytest.mark.parametrize("model_value", RECORD_MODELS)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        ({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}),
        ({"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"}),
        (({"dbName": "medRxiv", "identifier": f"{TEST_MEDRXIV_IDENTIFIER}"})),
    ],
    indirect=["mock_publication_fetch"],
)
def test_record_publication_identifier_statistics(
    client, setup_router_db, model_value, data_files, mock_publication_fetch
):
    """Test record model statistics for publication identifiers endpoint for published experiments and score sets."""
    mocked_publication = mock_publication_fetch
    record_update = {"primaryPublicationIdentifiers": [mocked_publication]}
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/publication-identifiers")

    assert response.status_code == 200
    assert record_update["primaryPublicationIdentifiers"][0]["dbName"] in response.json()
    assert (
        record_update["primaryPublicationIdentifiers"][0]["identifier"]
        in response.json()[record_update["primaryPublicationIdentifiers"][0]["dbName"]]
    )
    assert (
        response.json()[record_update["primaryPublicationIdentifiers"][0]["dbName"]][
            record_update["primaryPublicationIdentifiers"][0]["identifier"]
        ]
        == 1
    )


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_keyword_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for keywords endpoint for published experiments and score sets."""
    record_update = {"keywords": ["test_keyword"]}
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/keywords")

    assert response.status_code == 200
    assert "test_keyword" in response.json()
    assert response.json()["test_keyword"] == 1


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_doi_identifier_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for DOI identifiers for published experiments and score sets."""
    record_update = {
        "doiIdentifiers": [{"identifier": "10.17605/OSF.IO/75B2M", "url": "https://doi.org/10.17605/OSF.IO/75B2M"}]
    }
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/doi-identifiers")

    assert response.status_code == 200
    assert record_update["doiIdentifiers"][0]["identifier"] in response.json()
    assert response.json()[record_update["doiIdentifiers"][0]["identifier"]] == 1


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_raw_read_identifier_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for raw read identifiers for (un)published experiments and score sets."""
    record_update = {
        "rawReadIdentifiers": [{"identifier": "SRP002725", "url": "http://www.ebi.ac.uk/ena/data/view/SRP002725"}]
    }
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/raw-read-identifiers")

    assert response.status_code == 200

    # Raw Read Identifiers are not defined on score sets.
    if model_value != "score-set":
        assert record_update["rawReadIdentifiers"][0]["identifier"] in response.json()
        assert response.json()[record_update["rawReadIdentifiers"][0]["identifier"]] == 1


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_statistics_invalid_field(client, model_value):
    """Test record model statistic response for an invalid field."""
    response = client.get(f"/api/v1/statistics/record/{model_value}/invalid-field")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "field"]
    assert response.json()["detail"][0]["ctx"]["enum_values"] == RECORD_SHARED_FIELDS


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_statistics_empty_field(client, model_value):
    """Test record model statistic response for an empty field."""
    response = client.get(f"/api/v1/statistics/record/{model_value}/")
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def test_record_statistics_invalid_record_and_field(client):
    """Test record model statistic response for an invalid model and field."""
    response = client.get("/api/v1/statistics/record/invalid-model/invalid-field")

    # The order of this list should be reliable.
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "model"]
    assert response.json()["detail"][0]["ctx"]["enum_values"] == RECORD_MODELS
    assert response.json()["detail"][1]["loc"] == ["path", "field"]
    assert response.json()["detail"][1]["ctx"]["enum_values"] == RECORD_SHARED_FIELDS
