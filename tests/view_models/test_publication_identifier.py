import pytest

from mavedb.view_models.publication_identifier import PublicationIdentifierCreate


def test_publication_identifier_create_pubmed_validator():
    # Test valid pubmed identifier
    valid_identifier = "20711111"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "20711111"


def test_publication_identifier_create_new_biorxiv_validator():
    # Test valid new form of biorxiv identifier
    valid_identifier = "2019.12.12.207222"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "2019.12.12.207222"


def test_publication_identifier_create_old_biorxiv_validator():
    # Test valid old form of biorxiv identifier
    valid_identifier = "207222"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "207222"


def test_publication_identifier_create_new_medrxiv_validator():
    # Test valid new form of medrxiv identifier
    valid_identifier = "2019.12.12.20733333"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "2019.12.12.20733333"


def test_publication_identifier_create_old_medrxiv_validator():
    # Test valid old form of medrxiv identifier (this is the same format as pubmed identifiers)
    valid_identifier = "20733333"
    pubmed_one = PublicationIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == "20733333"


def test_cannot_create_publication_identifier_with_invalid_identifier():
    # Test invalid identifier
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        PublicationIdentifierCreate(identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid DOI or a valid PubMed, bioRxiv, or medRxiv identifier." in str(
        exc_info.value
    )


def test_cannot_create_publication_identifier_with_invalid_db_name():
    # Test invalid db name
    valid_identifier = "20711111"
    invalid_db_name = "not_a_db"
    with pytest.raises(ValueError) as exc_info:
        PublicationIdentifierCreate(identifier=valid_identifier, db_name=invalid_db_name)
    assert (
        "The `db_name` key within the identifier attribute of the external publication identifier should take one of the following values"
        in str(exc_info.value)
    )
