from mavedb.view_models.pubmed_identifier import PubmedIdentifierCreate
from mavedb.lib.validation.exceptions import ValidationError
import pytest

def test_pubmed_identifier_create_validator(test_empty_db):
    # Test valid identifier
    valid_identifier = '20711111'
    pubmed_one = PubmedIdentifierCreate(identifier=valid_identifier)
    assert pubmed_one.identifier == '20711111'
    # Test invalid identifier
    invalid_identifier = 'not_an_identifier'
    with pytest.raises(ValueError):
        PubmedIdentifierCreate(identifier=invalid_identifier)

    """
    ValidationError can't work
    try:
        PubmedIdentifierCreate(identifier=invalid_identifier)
    except ValidationError as e:
        assert str(e) == '{} is not a valid PubMed identifier.'.format(invalid_identifier)
    """