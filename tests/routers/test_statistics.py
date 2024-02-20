import pytest
from humps import camelize
from tests.helpers.util import (
    create_experiment,
    create_seq_score_set,
    create_acc_score_set,
    create_seq_score_set_with_variants,
    create_acc_score_set_with_variants,
    publish_score_set,
)
from tests.helpers.constants import TEST_MINIMAL_ACC_SCORESET, TEST_MINIMAL_SEQ_SCORESET

TARGET_ACCESSION_FIELDS = ["accession", "assembly", "gene"]
TARGET_SEQUENCE_FIELDS = ["sequence", "sequence-type"]
TARGET_GENE_FIELDS = ["category", "organism", "reference"]
TARGET_GENE_IDENTIFIER_FIELDS = ["ensembl-identifier", "refseq-identifier", "uniprot-identifier"]

RECORD_MODELS = ["experiment", "score-set"]
RECORD_SHARED_FIELDS = ["publication-identifiers", "keywords", "doi-identifiers", "raw-read-identifiers"]


## Test base case empty database responses for each statistic endpoint.


def test_empty_database_statistics(client):
    response = client.get("/api/v1/statistics/target/accession/accession")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/accession/assembly")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/accession/gene")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/sequence/sequence")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/sequence/sequence-type")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/category")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/organism")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/reference")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/ensembl-identifier")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/refseq-identifier")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/target/gene/uniprot-identifier")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/experiment/publication-identifiers")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/experiment/keywords")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/experiment/doi-identifiers")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/experiment/raw-read-identifiers")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/score-set/publication-identifiers")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/score-set/keywords")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/score-set/doi-identifiers")
    assert response.status_code == 200
    assert response.json() == {}

    response = client.get("/api/v1/statistics/record/score-set/raw-read-identifiers")
    assert response.status_code == 200
    assert response.json() == {}


## Test target accession statistics


@pytest.mark.parametrize(
    "field_value",
    TARGET_ACCESSION_FIELDS,
)
def test_target_accession_statistics_unpublished(client, setup_router_db, field_value):
    """Test target accession statistics do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_acc_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/statistics/target/accession/{field_value}")
    camelized_field_value = camelize(field_value.replace("-", "_"))

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value] not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize(
    "field_value",
    TARGET_ACCESSION_FIELDS,
)
def test_target_accession_statistics(client, setup_router_db, field_value, data_files):
    """Test target accession statistics endpoint for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_acc_score_set_with_variants(client, experiment["urn"], data_files / "scores_acc.csv")
    response = client.get(f"/api/v1/statistics/target/accession/{field_value}?only_published=False")
    camelized_field_value = camelize(field_value.replace("-", "_"))

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/target/accession/{field_value}?only_published=True")

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
def test_target_sequence_statistics_unpublished(client, setup_router_db, field_value):
    """Test target sequence statistics do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])
    response = client.get(f"/api/v1/statistics/target/sequence/{field_value}")
    camelized_field_value = camelize(field_value.replace("-", "_"))

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value] not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize(
    "field_value",
    TARGET_SEQUENCE_FIELDS,
)
def test_target_sequence_statistics(client, setup_router_db, field_value, data_files):
    """Test target sequence statistics endpoint for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    response = client.get(f"/api/v1/statistics/target/sequence/{field_value}?only_published=False")
    camelized_field_value = camelize(field_value.replace("-", "_"))

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/target/sequence/{field_value}?only_published=True")

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


# NOTE: It's clearer to not parametrize target gene statistics due to non-standard behavior between target accession
#       and target sequence subtypes.
def test_target_gene_category_statistics_acc_unpublished(client, setup_router_db):
    """Test target gene category statistics for accession based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_acc_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/category")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] not in response.json()
    assert response.json() == {}


def test_target_gene_category_statistics_acc(client, setup_router_db, data_files):
    """Test target gene category endpoint on accession based targest for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_acc_score_set_with_variants(client, experiment["urn"], data_files / "scores_acc.csv")
    response = client.get("/api/v1/statistics/target/gene/category?only_published=False")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/category?only_published=True")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]] == 1


def test_target_gene_category_statistics_seq_unpublished(client, setup_router_db):
    """Test target gene category statistics for sequence based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/category")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["category"] not in response.json()
    assert response.json() == {}


def test_target_gene_category_statistics_seq(client, setup_router_db, data_files):
    """Test target gene category endpoint on sequence based targest for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    response = client.get("/api/v1/statistics/target/gene/category?only_published=False")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["category"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/category?only_published=True")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]] == 1


def test_target_gene_organism_statistics_acc_unpublished(client, setup_router_db):
    """Test target gene organism statistcs for accession based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_acc_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/organism")

    assert response.status_code == 200
    assert response.json() == {}


def test_target_gene_organism_statistics_acc(client, setup_router_db, data_files):
    """Test target gene organism endpoint on accession based targets for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_acc_score_set_with_variants(client, experiment["urn"], data_files / "scores_acc.csv")
    response = client.get("/api/v1/statistics/target/gene/organism?only_published=False")

    assert "Homo sapiens" in response.json()
    assert response.json()["Homo sapiens"] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/organism?only_published=True")

    assert "Homo sapiens" in response.json()
    assert response.json()["Homo sapiens"] == 1


def test_target_gene_organism_statistics_seq_unpublished(client, setup_router_db):
    """Test target gene organism statistics for sequence based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/organism")

    assert response.status_code == 200
    assert (
        TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"]
        not in response.json()
    )
    assert response.json() == {}


def test_target_gene_organism_statistics_seq(client, setup_router_db, data_files):
    """Test target gene organism statistics on sequence based targets for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    response = client.get("/api/v1/statistics/target/gene/organism?only_published=False")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"] in response.json()
    assert (
        response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"]] == 1
    )

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/organism?only_published=True")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"] in response.json()
    assert (
        response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["organismName"]] == 1
    )


def test_target_gene_reference_statistics_acc_unpublished(client, setup_router_db):
    """Test target gene reference statistics for accession based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_acc_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/reference")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"] not in response.json()
    assert response.json() == {}


def test_target_gene_reference_statistics_acc(client, setup_router_db, data_files):
    """Test target gene reference statistics on accession based targets for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_acc_score_set_with_variants(client, experiment["urn"], data_files / "scores_acc.csv")
    response = client.get("/api/v1/statistics/target/gene/reference?only_published=False")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/reference?only_published=True")

    assert response.status_code == 200
    assert TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"] in response.json()
    assert response.json()[TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"]["assembly"]] == 1


def test_target_gene_reference_statistics_seq_unpublished(client, setup_router_db):
    """Test target gene reference statistics for sequence based targets do not include unpublished score sets."""
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])
    response = client.get("/api/v1/statistics/target/gene/reference")

    assert response.status_code == 200
    assert (
        TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"] not in response.json()
    )
    assert response.json() == {}


def test_target_gene_reference_statistics_seq(client, setup_router_db, data_files):
    """Test target gene reference statistics on sequence based targets for (un)published score sets."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv")
    response = client.get("/api/v1/statistics/target/gene/reference?only_published=False")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get("/api/v1/statistics/target/gene/reference?only_published=True")

    assert response.status_code == 200
    assert TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"] in response.json()
    assert response.json()[TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["reference"]["shortName"]] == 1


# Target gene identifier based statistics behave similarly enough to parametrize.
@pytest.mark.parametrize("field_value", TARGET_GENE_IDENTIFIER_FIELDS)
def test_target_gene_identifier_statistiscs_unpublished(client, setup_router_db, field_value):
    """Test target gene identifier statistics for target genes do not include unpublished score sets."""
    # Helper map that simplifies parametrization.
    test_mapper = {
        "ensembl-identifier": {"offset": 0, "identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}},
        "refseq-identifier": {"offset": 0, "identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}},
        "uniprot-identifier": {"offset": 0, "identifier": {"dbName": "UniProt", "identifier": "Q9Y617"}},
    }

    experiment = create_experiment(client)

    # Update each of the score set target genes to include our test external identifiers.
    seq_target = TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0].copy()
    seq_target.update({"externalIdentifiers": [v for v in test_mapper.values()]})
    create_seq_score_set(client, experiment["urn"], {"targetGenes": [seq_target]})

    acc_target = TEST_MINIMAL_ACC_SCORESET["targetGenes"][0].copy()
    acc_target.update({"externalIdentifiers": [v for v in test_mapper.values()]})
    create_acc_score_set(client, experiment["urn"], {"targetGenes": [acc_target]})

    response = client.get(f"/api/v1/statistics/target/gene/{field_value}")

    assert response.status_code == 200
    assert test_mapper[field_value]["identifier"]["identifier"] not in response.json()

    # For completeness, both an accession based and sequence based scoreset were created for this test.
    assert response.json() == {}


@pytest.mark.parametrize("field_value", TARGET_GENE_IDENTIFIER_FIELDS)
def test_target_gene_identifier_statistiscs(client, setup_router_db, field_value, data_files):
    """Test target gene identifier statistics endpoint for (un)published score sets."""
    # Helper map that simplifies parametrization.
    test_mapper = {
        "ensembl-identifier": {"offset": 0, "identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}},
        "refseq-identifier": {"offset": 0, "identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}},
        "uniprot-identifier": {"offset": 0, "identifier": {"dbName": "UniProt", "identifier": "Q9Y617"}},
    }

    experiment = create_experiment(client)

    # Update each of the score set target genes to include our test external identifiers.
    seq_target = TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0].copy()
    seq_target.update({"externalIdentifiers": [v for v in test_mapper.values()]})
    seq_score_set = create_seq_score_set_with_variants(
        client, experiment["urn"], data_files / "scores.csv", {"targetGenes": [seq_target]}
    )

    acc_target = TEST_MINIMAL_ACC_SCORESET["targetGenes"][0].copy()
    acc_target.update({"externalIdentifiers": [v for v in test_mapper.values()]})
    acc_score_set = create_acc_score_set_with_variants(
        client, experiment["urn"], data_files / "scores_acc.csv", {"targetGenes": [acc_target]}
    )

    response = client.get(f"/api/v1/statistics/target/gene/{field_value}?only_published=False")

    # For completeness, both an accession based and sequence based scoreset were created for this test.
    assert response.status_code == 200
    assert test_mapper[field_value]["identifier"]["identifier"] in response.json()
    assert response.json()[test_mapper[field_value]["identifier"]["identifier"]] == 2

    publish_score_set(client, seq_score_set["urn"])
    publish_score_set(client, acc_score_set["urn"])
    response = client.get(f"/api/v1/statistics/target/gene/{field_value}?only_published=True")

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
def test_record_publication_identifier_statistics_unpublished(client, setup_router_db, model_value):
    """Test record model statistics for publication identifiers do not include unpublished experiements or score sets."""
    record_update = {"primaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": "20711194"}]}
    experiment = create_experiment(client, record_update)
    create_seq_score_set(client, experiment["urn"], record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/publication-identifiers")

    assert response.status_code == 200
    assert record_update["primaryPublicationIdentifiers"][0]["dbName"] not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_publication_identifier_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for publication identifiers endpoint for (un)published experiments and score sets."""
    record_update = {"primaryPublicationIdentifiers": [{"dbName": "PubMed", "identifier": "20711194"}]}
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/publication-identifiers?only_published=False")

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

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/publication-identifiers?only_published=True")

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
def test_record_keyword_statistics_unpublished(client, setup_router_db, model_value):
    """Test record model statistics for keywords do not include unpublished experiments or score sets."""
    record_update = {"keywords": ["test_keyword"]}
    experiment = create_experiment(client, record_update)
    create_seq_score_set(client, experiment["urn"], record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/keywords")

    assert response.status_code == 200
    assert "test_keyword" not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_keyword_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for keywords endpoint for (un)published experiments and score sets."""
    record_update = {"keywords": ["test_keyword"]}
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/keywords?only_published=False")

    assert response.status_code == 200
    assert "test_keyword" in response.json()
    assert response.json()["test_keyword"] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/keywords?only_published=True")

    assert response.status_code == 200
    assert "test_keyword" in response.json()
    assert response.json()["test_keyword"] == 1


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_doi_identifier_statistics_unpublished(client, setup_router_db, model_value):
    """Test record model statistics for DOI identifiers do not include unpublished experiments or score sets."""
    record_update = {
        "doiIdentifiers": [{"identifier": "10.17605/OSF.IO/75B2M", "url": "https://doi.org/10.17605/OSF.IO/75B2M"}]
    }
    experiment = create_experiment(client, record_update)
    create_seq_score_set(client, experiment["urn"], record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/doi-identifiers")

    assert response.status_code == 200
    assert record_update["doiIdentifiers"][0]["identifier"] not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_doi_identifier_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for DOI identifiers for (un)published experiments and score sets."""
    record_update = {
        "doiIdentifiers": [{"identifier": "10.17605/OSF.IO/75B2M", "url": "https://doi.org/10.17605/OSF.IO/75B2M"}]
    }
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)

    response = client.get(f"/api/v1/statistics/record/{model_value}/doi-identifiers?only_published=False")

    assert response.status_code == 200
    assert record_update["doiIdentifiers"][0]["identifier"] in response.json()
    assert response.json()[record_update["doiIdentifiers"][0]["identifier"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/doi-identifiers?only_published=True")

    assert response.status_code == 200
    assert record_update["doiIdentifiers"][0]["identifier"] in response.json()
    assert response.json()[record_update["doiIdentifiers"][0]["identifier"]] == 1


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_raw_read_identifier_statistics_unpublished(client, setup_router_db, model_value):
    """Test record model statistics for raw read identifiers do not include unpublished experiments or score sets."""
    record_update = {
        "rawReadIdentifiers": [{"identifier": "SRP002725", "url": "http://www.ebi.ac.uk/ena/data/view/SRP002725"}]
    }
    experiment = create_experiment(client, record_update)
    create_seq_score_set(client, experiment["urn"], record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/raw-read-identifiers")

    assert response.status_code == 200
    assert record_update["rawReadIdentifiers"][0]["identifier"] not in response.json()
    assert response.json() == {}


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_raw_read_identifier_statistics(client, setup_router_db, model_value, data_files):
    """Test record model statistics for raw read identifiers for (un)published experiments and score sets."""
    record_update = {
        "rawReadIdentifiers": [{"identifier": "SRP002725", "url": "http://www.ebi.ac.uk/ena/data/view/SRP002725"}]
    }
    experiment = create_experiment(client, record_update)
    score_set = create_seq_score_set_with_variants(client, experiment["urn"], data_files / "scores.csv", record_update)
    response = client.get(f"/api/v1/statistics/record/{model_value}/raw-read-identifiers?only_published=False")

    assert response.status_code == 200

    # Raw Read Identifiers are not defined on score sets.
    if model_value != "score-set":
        assert record_update["rawReadIdentifiers"][0]["identifier"] in response.json()
        assert response.json()[record_update["rawReadIdentifiers"][0]["identifier"]] == 1

    publish_score_set(client, score_set["urn"])
    response = client.get(f"/api/v1/statistics/record/{model_value}/raw-read-identifiers?only_published=True")

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
