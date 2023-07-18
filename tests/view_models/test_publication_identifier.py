from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
import pytest


def test_publication_identifier_create_pubmed_validator(client):
    # Test valid pubmed identifier
    valid_identifier = "20711111"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "20711111"


def test_publication_identifier_create_new_biorxiv_validator(client):
    # Test valid new form of biorxiv identifier
    valid_identifier = "2019.12.12.207222"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "2019.12.12.207222"


def test_publication_identifier_create_old_biorxiv_validator(client):
    # Test valid old form of biorxiv identifier
    valid_identifier = "207222"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "207222"


def test_publication_identifier_create_new_medrxiv_validator(client):
    # Test valid new form of medrxiv identifier
    valid_identifier = "2019.12.12.20733333"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "2019.12.12.20733333"


def test_publication_identifier_create_old_medrxiv_validator(client):
    # Test valid old form of medrxiv identifier (this is the same format as pubmed identifiers)
    valid_identifier = "20733333"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "20733333"


def test_invalid_publication_identifier_create_validator(client):
    # Test invalid identifier
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        PublicationIdentifierCreate(identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid PubMed, bioRxiv, or medRxiv identifier." in str(exc_info.value)


def test_invalid_publication_identifier_date_part_create_validator(client):
    # Test invalid identifier (date too early on bioRxiv identifier)
    invalid_identifier = "2018.12.12.207222"
    with pytest.raises(ValueError) as exc_info:
        PublicationIdentifierCreate(identifier=invalid_identifier)
    assert "'2018.12.12.207222' is not a valid PubMed, bioRxiv, or medRxiv identifier." in str(exc_info.value)
