import urllib.parse

from mavedb.lib.annotation.document import (
    experiment_as_iri,
    experiment_to_document,
    score_set_as_iri,
    score_set_to_document,
    mapped_variant_as_iri,
    mapped_variant_to_document,
    variant_as_iri,
    variant_to_document,
)

BASE_URL = "https://mavedb.org"


def test_experiment_as_iri(mock_experiment):
    expected_iri_root = f"{BASE_URL}/experiments/{mock_experiment.urn}"
    assert experiment_as_iri(mock_experiment).root == expected_iri_root


def test_experiment_to_document(mock_experiment):
    document = experiment_to_document(mock_experiment)

    assert document.id == mock_experiment.urn
    assert document.name == "MaveDB Experiment"
    assert document.title == mock_experiment.title
    assert document.description == mock_experiment.short_description
    assert document.documentType == "experiment"
    assert len(document.urls) > 0
    assert experiment_as_iri(mock_experiment).root in document.urls


def test_score_set_as_iri(mock_score_set):
    expected_iri_root = f"{BASE_URL}/score-sets/{mock_score_set.urn}"
    assert score_set_as_iri(mock_score_set).root == expected_iri_root


def test_score_set_to_document(mock_score_set):
    document = score_set_to_document(mock_score_set)

    assert document.id == mock_score_set.urn
    assert document.name == "MaveDB Score Set"
    assert document.title == mock_score_set.title
    assert document.description == mock_score_set.short_description
    assert document.documentType == "score set"
    assert len(document.urls) > 0
    assert score_set_as_iri(mock_score_set).root in document.urls


def test_mapped_variant_as_iri(mock_mapped_variant):
    expected_iri_root = f"https://mavedb.org/variant/{urllib.parse.quote_plus(mock_mapped_variant.clingen_allele_id)}"
    assert mapped_variant_as_iri(mock_mapped_variant).root == expected_iri_root


def test_mapped_variant_to_document(mock_mapped_variant):
    document = mapped_variant_to_document(mock_mapped_variant)

    assert document.id == mock_mapped_variant.variant.urn
    assert document.name == "MaveDB Mapped Variant"
    assert document.documentType == "mapped genomic variant description"
    assert len(document.urls) > 0
    assert mapped_variant_as_iri(mock_mapped_variant).root in document.urls


def test_mapped_variant_as_iri_no_caid(mock_mapped_variant):
    mock_mapped_variant.clingen_allele_id = None
    assert mapped_variant_as_iri(mock_mapped_variant) is None


def test_mapped_variant_to_document_no_caid(mock_mapped_variant):
    mock_mapped_variant.clingen_allele_id = None
    document = mapped_variant_to_document(mock_mapped_variant)
    assert document is None


def test_variant_as_iri(mock_variant):
    expected_iri_root = f"https://mavedb.org/score-sets/{mock_variant.score_set.urn}?variant={urllib.parse.quote_plus(mock_variant.urn)}"
    assert variant_as_iri(mock_variant).root == expected_iri_root


def test_variant_to_document(mock_variant):
    document = variant_to_document(mock_variant)

    assert document.id == mock_variant.urn
    assert document.name == "MaveDB Variant"
    assert document.documentType == "genomic variant description"
    assert len(document.urls) > 0
    assert variant_as_iri(mock_variant).root in document.urls
