from mavedb.view_models.pubmed_identifier import PubmedIdentifierCreate
from mavedb.lib.validation.exceptions import ValidationError
import pytest

def test_pubmed_identifier_create_validator(test_empty_db):
    # Test valid identifier
    valid_identifier = '20711111'
    pubmed_one = PubmedIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == '20711111'

def test_invalid_pubmed_identifier_create_validator(test_empty_db):
    # Test invalid identifier
    invalid_identifier = 'not_an_identifier'
    with pytest.raises(ValueError) as exc_info:
        PubmedIdentifierCreate(identifier=invalid_identifier)
    assert "not_an_identifier is not a valid PubMed identifier." in str(exc_info.value)