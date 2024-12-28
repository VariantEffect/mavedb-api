import pytest

from mavedb.view_models.external_gene_identifier_offset import ExternalGeneIdentifierOffsetCreate


def test_create_external_identifier_with_offset():
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    externalIdentifier = ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=1)
    assert externalIdentifier.offset == 1


def test_cannot_create_external_identifier_with_create_identifier_with_string_offset():
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    offset = "invalid"
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=offset)
    assert "Input should be a valid integer" in str(exc_info.value)


def test_cannot_create_external_identifier_with_create_identifier_with_negative_offset():
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    offset = -10
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierOffsetCreate(identifier=identifier, offset=offset)
    assert "Offset should not be a negative number" in str(exc_info.value)


def test_cannot_create_external_identifier_with_none_offset():
    identifier = {"db_name": "RefSeq", "identifier": "NM_003345"}
    offset = None
    with pytest.raises(ValueError) as exc_info:
        ExternalGeneIdentifierOffsetCreate(db_name=identifier, offset=offset)
    assert "Field required" in str(exc_info.value)
