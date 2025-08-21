# ruff: noqa: E402

import pytest
from humps import camelize
from unittest.mock import patch

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.published_variant import PublishedVariantsMV

from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_NT_CDOT_TRANSCRIPT,
    TEST_KEYWORDS,
    TEST_MEDRXIV_IDENTIFIER,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_PUBMED_IDENTIFIER,
    VALID_GENE,
)
from tests.helpers.util.score_set import publish_score_set, create_acc_score_set, create_seq_score_set
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.variant import mock_worker_variant_insertion, create_mapped_variants_for_score_set

TARGET_ACCESSION_FIELDS = ["accession", "assembly", "gene"]
TARGET_SEQUENCE_FIELDS = ["sequence", "sequence-type"]
TARGET_GENE_FIELDS = ["category", "organism"]
TARGET_GENE_IDENTIFIER_FIELDS = ["ensembl-identifier", "refseq-identifier", "uniprot-identifier"]

RECORD_MODELS = ["experiment", "score-set"]
RECORD_SHARED_FIELDS = ["publication-identifiers", "keywords", "doi-identifiers", "raw-read-identifiers", "created-by"]

EXTERNAL_IDENTIFIERS = {
    "ensembl-identifier": {"offset": 0, "identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}},
    "refseq-identifier": {"offset": 0, "identifier": {"dbName": "RefSeq", "identifier": "NM_003345"}},
    "uniprot-identifier": {"offset": 0, "identifier": {"dbName": "UniProt", "identifier": "Q9Y617"}},
}


# Fixtures for setting up score sets on which to calculate statistics.
# Adds an experiment and score set to the database, then publishes the score set.
@pytest.fixture
def setup_acc_scoreset(setup_router_db, session, data_provider, client, data_files):
    experiment = create_experiment(client)
    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_NT_CDOT_TRANSCRIPT
    ):
        score_set = create_acc_score_set(client, experiment["urn"])
        score_set = mock_worker_variant_insertion(
            client, session, data_provider, score_set, data_files / "scores_acc.csv"
        )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, score_set["urn"])
        worker_queue.assert_called_once()


@pytest.fixture
def setup_seq_scoreset(setup_router_db, session, data_provider, client, data_files):
    experiment = create_experiment(client)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"])
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )
    create_mapped_variants_for_score_set(session, unpublished_score_set["urn"], TEST_MINIMAL_MAPPED_VARIANT)

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    # Note that we have not created indexes for this view when it is generated via metadata. This differs
    # from the database created via alembic, which does create indexes.
    PublishedVariantsMV.refresh(session, False)


def assert_statistic(desired_field_value, response):
    """
    Each statistic test must check that the response code was 200,
    that the desired field value is present in the response, and that
    the desired field value exists exactly once in the test data.
    """

    # Assert the GET request succeeded, that the desired field was present in the response, and that the number of times the field
    # appears in the database is 1. Tests calling this method should have created one score set containing the desired field.
    assert response.status_code == 200
    assert (
        desired_field_value in response.json()
    ), f"Target accession statistic {desired_field_value} not present in statistic response."
    assert (
        response.json()[desired_field_value] == 1
    ), f"Target accession statistic {desired_field_value} should appear on one (and only one) test score set."


def add_query_param(url, query_name, query_value):
    """Add a group value to the URL if one is provided."""
    if query_name and query_value:
        return f"{url}?{query_name}={query_value}"

    return url


####################################################################################################
# Test empty database statistics
####################################################################################################


@pytest.mark.parametrize(
    "stats_endpoint",
    (
        "target/accession/accession",
        "target/accession/assembly",
        "target/accession/gene",
        "target/sequence/sequence",
        "target/sequence/sequence-type",
        "target/gene/category",
        "target/gene/organism",
        "target/gene/ensembl-identifier",
        "target/gene/uniprot-identifier",
        "target/gene/refseq-identifier",
        "record/experiment/publication-identifiers",
        "record/experiment/keywords",
        "record/experiment/doi-identifiers",
        "record/experiment/raw-read-identifiers",
        "record/experiment/created-by",
        "record/score-set/publication-identifiers",
        "record/score-set/doi-identifiers",
        "record/score-set/raw-read-identifiers",
        "record/score-set/created-by",
    ),
)
def test_empty_database_statistics(client, stats_endpoint):
    response = client.get(f"/api/v1/statistics/{stats_endpoint}")
    assert response.status_code == 200, f"Non-200 status code for endpoint {stats_endpoint}."
    assert response.json() == {}, f"Non-empty response for endpoint {stats_endpoint}."


####################################################################################################
# Test target accession statistics
####################################################################################################


@pytest.mark.parametrize(
    "field_value",
    TARGET_ACCESSION_FIELDS,
)
def test_target_accession_statistics(client, field_value, setup_acc_scoreset):
    """Test target accession statistics endpoint for published score sets."""
    camelized_field_value = camelize(field_value.replace("-", "_"))
    response = client.get(f"/api/v1/statistics/target/accession/{field_value}")
    desired_field_value = TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["targetAccession"][camelized_field_value]
    assert_statistic(desired_field_value, response)


def test_target_accession_invalid_field(client):
    """Test target accession statistic response for an invalid target accession field."""
    response = client.get("/api/v1/statistics/target/accession/invalid-field")
    assert response.status_code == 404


def test_target_accession_empty_field(client):
    """Test target accession statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/accession/")
    assert response.status_code == 404


####################################################################################################
# Test target sequence statistics
####################################################################################################


@pytest.mark.parametrize(
    "field_value",
    TARGET_SEQUENCE_FIELDS,
)
def test_target_sequence_statistics(client, field_value, setup_seq_scoreset):
    """Test target sequence statistics endpoint for published score sets."""
    camelized_field_value = camelize(field_value.replace("-", "_"))
    response = client.get(f"/api/v1/statistics/target/sequence/{field_value}")
    desired_field_value = TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"][camelized_field_value]
    assert_statistic(desired_field_value, response)


def test_target_sequence_invalid_field(client):
    """Test target sequence statistic response for an invalid field."""
    response = client.get("/api/v1/statistics/target/sequence/invalid-field")
    assert response.status_code == 404


def test_target_sequence_empty_field(client):
    """Test target sequence statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/sequence/")
    assert response.status_code == 404


####################################################################################################
# Test target gene statistics
####################################################################################################


# Desired values live in different spots for fields on target genes because of the differing target sequence
# and target accession sub types. Set up each desired field separately due to this potential issue.
@pytest.mark.parametrize(
    "field,desired_field_value",
    [
        ("category", TEST_MINIMAL_ACC_SCORESET["targetGenes"][0]["category"]),
        ("organism", "Homo sapiens"),
    ],
)
def test_target_gene_field_statistics_acc(client, field, desired_field_value, setup_acc_scoreset, request):
    """Test target gene category endpoint on accession based target for published score sets."""
    response = client.get(f"/api/v1/statistics/target/gene/{field}")
    assert_statistic(desired_field_value, response)


@pytest.mark.parametrize(
    "field,desired_field_value",
    [
        ("category", TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["category"]),
        ("organism", TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0]["targetSequence"]["taxonomy"]["organismName"]),
    ],
)
def test_target_gene_field_statistics_seq(client, field, desired_field_value, setup_seq_scoreset, request):
    """Test target gene category endpoint on accession based target for published score sets."""
    response = client.get(f"/api/v1/statistics/target/gene/{field}")
    assert_statistic(desired_field_value, response)


# Target gene identifier based statistics behave similarly enough to parametrize.
@pytest.mark.parametrize("field_value", TARGET_GENE_IDENTIFIER_FIELDS)
@pytest.mark.parametrize(
    "target", (TEST_MINIMAL_ACC_SCORESET["targetGenes"][0], TEST_MINIMAL_SEQ_SCORESET["targetGenes"][0])
)
def test_target_gene_identifier_statistiscs(
    session, data_provider, client, setup_router_db, data_files, field_value, target
):
    """Test target gene identifier statistics endpoint for published score sets."""
    record_update = {"externalIdentifiers": [v for v in EXTERNAL_IDENTIFIERS.values()]}
    target.update(record_update)

    # Create whichever score set target type is being tested.
    experiment = create_experiment(client)
    if "targetAccession" in target:
        with patch.object(
            cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_NT_CDOT_TRANSCRIPT
        ):
            unpublished_score_set = create_acc_score_set(client, experiment["urn"])
            unpublished_score_set = mock_worker_variant_insertion(
                client, session, data_provider, unpublished_score_set, data_files / "scores_acc.csv"
            )

    elif "targetSequence" in target:
        unpublished_score_set = create_seq_score_set(client, experiment["urn"])
        unpublished_score_set = mock_worker_variant_insertion(
            client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
        )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/statistics/target/gene/{field_value}")
    desired_field_value = EXTERNAL_IDENTIFIERS[field_value]["identifier"]["identifier"]
    assert_statistic(desired_field_value, response)


def test_target_gene_invalid_field(client):
    """Test target gene statistic response for an invalid field."""
    response = client.get("/api/v1/statistics/target/gene/invalid-field")
    assert response.status_code == 404


def test_target_gene_empty_field(client):
    """Test target gene statistic response for an empty field."""
    response = client.get("/api/v1/statistics/target/gene/")
    assert response.status_code == 404


####################################################################################################
# Test mapped target gene statistics
####################################################################################################


def test_mapped_target_gene_counts(client, setup_router_db, setup_seq_scoreset):
    """Test mapped target gene counts endpoint for published score sets."""
    response = client.get("/api/v1/statistics/target/mapped/gene")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    assert len(response.json().keys()) == 1
    assert response.json()[VALID_GENE] == 1


####################################################################################################
# Test record statistics
####################################################################################################


@pytest.mark.parametrize("model_value", RECORD_MODELS)
@pytest.mark.parametrize(
    "mock_publication_fetch",
    [
        ({"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"}),
        ({"dbName": "bioRxiv", "identifier": f"{TEST_BIORXIV_IDENTIFIER}"}),
        ({"dbName": "medRxiv", "identifier": f"{TEST_MEDRXIV_IDENTIFIER}"}),
    ],
    indirect=["mock_publication_fetch"],
)
def test_record_publication_identifier_statistics(
    session, data_provider, client, setup_router_db, model_value, data_files, mock_publication_fetch
):
    """Test record model statistics for publication identifiers endpoint for published experiments and score sets."""
    mocked_publication = mock_publication_fetch

    # Create experiment and score set resources. The fixtures are more useful for the simple cases that don't need scoreset / experiment
    # updates. Folding these more complex setup steps into a fixture is more trouble than it's worth.
    record_update = {"primaryPublicationIdentifiers": [mocked_publication]}
    experiment = create_experiment(client, record_update)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"], record_update)
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/statistics/record/{model_value}/publication-identifiers")

    # Have to check this response more carefully since it is structured like {"dbName": {"identifier": count}}
    # rather than {"identifier": count} to avoid issues with duplicated identifiers in different DBs.
    desired_db_value = record_update["primaryPublicationIdentifiers"][0]["dbName"]
    desired_field_value = record_update["primaryPublicationIdentifiers"][0]["identifier"]

    assert response.status_code == 200
    assert desired_db_value in response.json()
    assert desired_field_value in response.json()[desired_db_value]
    assert response.json()[desired_db_value][desired_field_value] == 1


def test_record_keyword_statistics(session, data_provider, client, setup_router_db, data_files):
    """
    Test record model statistics for keywords endpoint for published experiments.
    Score set does not have controlled keyword.
    Experiment._find_keyword does not have create new keyword function anymore.
    Hence we need temporary keywords in mock database to do the test.
    """
    record_update = {"keywords": TEST_KEYWORDS}
    # Create experiment and score set resources. The fixtures are more useful for the simple cases that don't need scoreset / experiment
    # updates. Folding these more complex setup steps into a fixture is more trouble than it's worth.
    experiment = create_experiment(client, record_update)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"], record_update)
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get("/api/v1/statistics/record/experiment/keywords")
    desired_field_values = ["SaCas9", "Endogenous locus library method", "Base editor", "Other"]
    for desired_field_value in desired_field_values:
        assert_statistic(desired_field_value, response)


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_doi_identifier_statistics(session, data_provider, client, setup_router_db, model_value, data_files):
    """Test record model statistics for DOI identifiers for published experiments and score sets."""
    record_update = {
        "doiIdentifiers": [{"identifier": "10.17605/OSF.IO/75B2M", "url": "https://doi.org/10.17605/OSF.IO/75B2M"}]
    }

    # Create experiment and score set resources. The fixtures are more useful for the simple cases that don't need scoreset / experiment
    # updates. Folding these more complex setup steps into a fixture is more trouble than it's worth.
    experiment = create_experiment(client, record_update)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"], record_update)
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/statistics/record/{model_value}/doi-identifiers")
    desired_field_value = record_update["doiIdentifiers"][0]["identifier"]
    assert_statistic(desired_field_value, response)


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_raw_read_identifier_statistics(
    session, data_provider, client, setup_router_db, model_value, data_files
):
    """Test record model statistics for raw read identifiers for published experiments and score sets."""
    record_update = {
        "rawReadIdentifiers": [{"identifier": "SRP002725", "url": "http://www.ebi.ac.uk/ena/data/view/SRP002725"}]
    }

    # Create experiment and score set resources. The fixtures are more useful for the simple cases that don't need scoreset / experiment
    # updates. Folding these more complex setup steps into a fixture is more trouble than it's worth.
    experiment = create_experiment(client, record_update)
    unpublished_score_set = create_seq_score_set(client, experiment["urn"], record_update)
    unpublished_score_set = mock_worker_variant_insertion(
        client, session, data_provider, unpublished_score_set, data_files / "scores.csv"
    )

    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None) as worker_queue:
        publish_score_set(client, unpublished_score_set["urn"])
        worker_queue.assert_called_once()

    response = client.get(f"/api/v1/statistics/record/{model_value}/raw-read-identifiers")
    desired_field_value = record_update["rawReadIdentifiers"][0]["identifier"]

    # Raw Read Identifiers are not defined on score sets...
    if model_value != "score-set":
        assert_statistic(desired_field_value, response)

    # ...so the score-set response should be empty.
    else:
        assert response.status_code == 200
        assert response.json() == {}


@pytest.mark.parametrize("field_value", RECORD_SHARED_FIELDS)
def test_record_statistics_invalid_record(client, field_value):
    """Test record model statistic response for a record we don't provide statisticss on."""
    response = client.get(f"/api/v1/statistics/record/invalid-record/{field_value}")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "record"]
    assert all(enum_val in response.json()["detail"][0]["ctx"]["expected"] for enum_val in RECORD_MODELS)


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_statistics_invalid_field(client, model_value):
    """Test record model statistic response for a field we don't provide statisticss on."""
    response = client.get(f"/api/v1/statistics/record/{model_value}/invalid-field")
    assert response.status_code == 404


@pytest.mark.parametrize("model_value", RECORD_MODELS)
def test_record_statistics_empty_field(client, model_value):
    """Test record model statistic response for an empty field."""
    response = client.get(f"/api/v1/statistics/record/{model_value}/")
    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def test_record_statistics_invalid_record_and_field(client):
    """Test record model statistic response for an invalid model and field."""
    response = client.get("/api/v1/statistics/record/invalid-model/invalid-field")
    assert response.status_code == 404


@pytest.mark.parametrize("model_value", RECORD_MODELS)
@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_record_counts_no_published_data(client, model_value, group_value, setup_router_db):
    """Test record counts endpoint grouped by month and year for published experiments and score sets."""
    response = client.get(
        add_query_param(f"/api/v1/statistics/record/{model_value}/published/count", "group", group_value)
    )

    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 0


@pytest.mark.parametrize("model_value", RECORD_MODELS)
@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_record_counts_grouped(
    session, client, model_value, group_value, setup_router_db, setup_seq_scoreset, setup_acc_scoreset
):
    """Test record counts endpoint grouped by month and year for published experiments and score sets."""
    response = client.get(
        add_query_param(f"/api/v1/statistics/record/{model_value}/published/count", "group", group_value)
    )
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 2


def test_record_counts_invalid_model(client):
    """Test record counts endpoint with an invalid model."""
    response = client.get("/api/v1/statistics/record/invalid-model/published/count")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "model"]
    assert all(enum_val in response.json()["detail"][0]["ctx"]["expected"] for enum_val in RECORD_MODELS)


def test_record_counts_invalid_group(client):
    """Test record counts endpoint with an invalid group."""
    response = client.get("/api/v1/statistics/record/experiment/published/count?group=invalid-group")
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "group"]
    assert all(enum_val in response.json()["detail"][0]["ctx"]["expected"] for enum_val in ["month", "year"])


####################################################################################################
# Test variant statistics
####################################################################################################


@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_variant_counts(client, group_value, setup_router_db, setup_seq_scoreset):
    """Test variant counts endpoint for published variants."""
    response = client.get(add_query_param("/api/v1/statistics/variant/count", "group", group_value))
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 3


@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_variant_counts_no_published_data(client, group_value, setup_router_db):
    """Test variant counts endpoint with no published variants."""
    response = client.get(add_query_param("/api/v1/statistics/variant/count", "group", group_value))
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 0


@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_mapped_variant_counts_groups(client, group_value, setup_router_db, setup_seq_scoreset):
    """Test variant counts endpoint for published variants."""
    url_with_group = add_query_param("/api/v1/statistics/mapped-variant/count", "group", group_value)
    response = client.get(url_with_group)
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 3


@pytest.mark.parametrize("group_value", ["month", "year", None])
def test_mapped_variant_counts_groups_no_published_data(client, group_value, setup_router_db):
    """Test variant counts endpoint with no published variants."""
    url_with_group = add_query_param("/api/v1/statistics/mapped-variant/count", "group", group_value)
    response = client.get(url_with_group)
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 0


@pytest.mark.parametrize("current_value", [True, False])
def test_mapped_variant_counts_current(client, current_value, setup_router_db, setup_seq_scoreset):
    """Test variant counts endpoint for published variants."""
    url_with_current = add_query_param("/api/v1/statistics/mapped-variant/count", "current", current_value)
    response = client.get(url_with_current)
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 3


@pytest.mark.parametrize("current_value", [True, False])
def test_mapped_variant_counts_current_no_published_data(client, current_value, setup_router_db):
    """Test variant counts endpoint with no published variants."""
    url_with_current = add_query_param("/api/v1/statistics/mapped-variant/count", "current", current_value)
    response = client.get(url_with_current)
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

    for key, value in response.json().items():
        assert isinstance(key, str)
        assert value == 0
