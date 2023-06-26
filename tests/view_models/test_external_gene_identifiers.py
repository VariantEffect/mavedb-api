from mavedb.view_models.external_gene_identifier import ExternalGeneIdentifierCreate
from mavedb.view_models.external_gene_identifier_offset import ExternalGeneIdentifierOffsetCreate

import pytest


def test_create_ensemble_identifier(client):
    # Test valid identifier
    db_name = "Ensembl"
    identifier = "ENSG00000103275"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert externalIdentifier.db_name == "Ensembl"
    assert externalIdentifier.identifier == "ENSG00000103275"


def test_create_invalid_ensemble_identifier(client):
    # Test valid identifier
    db_name = "Ensembl"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid Ensembl accession." in str(exc_info.value)


def test_create_uniprot_identifier(client):
    db_name = "UniProt"
    identifier = "P63279"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert externalIdentifier.db_name == "UniProt"
    assert externalIdentifier.identifier == "P63279"


def test_create_invalid_uniprot_identifier(client):
    db_name = "UniProt"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid UniProt accession." in str(exc_info.value)


def test_create_refseq_identifier(client):
    db_name = "RefSeq"
    identifier = "NM_003345"
    externalIdentifier = ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert externalIdentifier.db_name == "RefSeq"
    assert externalIdentifier.identifier == "NM_003345"


def test_create_invalid_refseq_identifier(client):
    db_name = "RefSeq"
    invalid_identifier = "not_an_identifier"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=invalid_identifier)
    assert "'not_an_identifier' is not a valid RefSeq accession." in str(exc_info.value)


def test_empty_db_name(client):
    db_name = ""
    identifier = "ENSG00000103275"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert "none is not an allowed value" in str(exc_info.value)


def test_space_db_name(client):
    db_name = "   "
    identifier = "ENSG00000103275"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert "db_name should not be empty" in str(exc_info.value)


def test_none_db_name(client):
    db_name = None
    identifier = "ENSG00000103275"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert "none is not an allowed value" in str(exc_info.value)


def test_invalid_db_name(client):
    db_name = "Invalid"
    identifier = "ENSG00000103275"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierCreate(db_name=db_name, identifier=identifier)
    assert (
        "The `db_name` key within the identifier attribute of the external identifier should take one of "
        "the following values: ['UniProt', 'RefSeq', 'Ensembl']." in str(exc_info.value)
    )


def test_create_identifier_with_offset(client):
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    externalIdentifier = ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=1)
    assert externalIdentifier.offset == 1


def test_create_identifier_with_string_offset(client):
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    offset = "invalid"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=offset)
    assert "value is not a valid integer" in str(exc_info.value)


def test_create_identifier_with_negative_offset(client):
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=-10)
    assert "Offset should not be a negative number" in str(exc_info.value)
