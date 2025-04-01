import pytest

from mavedb.view_models.external_gene_identifier import ExternalGeneIdentifierCreate
from tests.helpers.constants import TEST_REFSEQ_IDENTIFIER, TEST_UNIPROT_IDENTIFIER, TEST_ENSEMBL_IDENTIFIER


def test_create_ensemble_identifier():
    # Test valid identifier
    db_name = "Ensembl"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_ENSEMBL_IDENTIFIER)
    assert externalIdentifier.db_name == "Ensembl"
    assert externalIdentifier.identifier == TEST_ENSEMBL_IDENTIFIER


def test_cannot_create_invalid_ensemble_identifier():
    db_name = "Ensembl"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid Ensembl accession." in str(exc_info.value)


def test_create_uniprot_identifier():
    db_name = "UniProt"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_UNIPROT_IDENTIFIER)
    assert externalIdentifier.db_name == "UniProt"
    assert externalIdentifier.identifier == TEST_UNIPROT_IDENTIFIER


def test_cannot_create_invalid_uniprot_identifier():
    db_name = "UniProt"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid UniProt accession." in str(exc_info.value)


def test_create_refseq_identifier():
    db_name = "RefSeq"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_REFSEQ_IDENTIFIER)
    assert externalIdentifier.db_name == "RefSeq"
    assert externalIdentifier.identifier == TEST_REFSEQ_IDENTIFIER


def test_cannot_create_invalid_refseq_identifier():
    db_name = "RefSeq"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid RefSeq accession." in str(exc_info.value)


def test_cannot_create_external_identifier_with_empty_db_name():
    db_name = ""
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_ENSEMBL_IDENTIFIER)
    assert "Input should be a valid string" in str(exc_info.value)


def test_cannot_create_external_identifier_with_space_db_name():
    db_name = "   "
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_ENSEMBL_IDENTIFIER)
    assert "db_name should not be empty" in str(exc_info.value)


def test_cannot_create_external_identifier_with_none_db_name():
    db_name = None
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_ENSEMBL_IDENTIFIER)
    assert "Input should be a valid string" in str(exc_info.value)


def test_cannot_create_external_identifier_with_invalid_db_name():
    db_name = "Invalid"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=TEST_ENSEMBL_IDENTIFIER)
    assert (
        "The `db_name` key within the identifier attribute of the external identifier should take one of "
        "the following values: ['UniProt', 'RefSeq', 'Ensembl']." in str(exc_info.value)
    )
