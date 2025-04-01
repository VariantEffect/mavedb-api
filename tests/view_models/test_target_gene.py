import pytest

from mavedb.view_models.target_gene import TargetGeneCreate, SavedTargetGene
from tests.helpers.constants import (
    SEQUENCE,
    TEST_POPULATED_TAXONOMY,
    TEST_SAVED_TAXONOMY,
    TEST_REFSEQ_EXTERNAL_IDENTIFIER,
    TEST_ENSEMBLE_EXTERNAL_IDENTIFIER,
    TEST_UNIPROT_EXTERNAL_IDENTIFIER,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict


def test_create_target_gene_with_sequence():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_sequence = {
        "sequenceType": "dna",
        "sequence": SEQUENCE,
        "taxonomy": TEST_POPULATED_TAXONOMY,
    }
    externalIdentifier = TargetGeneCreate(
        name=name,
        category=category,
        external_identifiers=external_identifiers,
        target_sequence=target_sequence,
    )
    assert externalIdentifier.name == "UBE2I"
    assert externalIdentifier.category == "regulatory"


def test_create_target_gene_with_accession():
    name = "BRCA1"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_accession = {"accession": "NM_001637.3", "assembly": "GRCh37", "gene": "BRCA1", "isBaseEditor": False}
    externalIdentifier = TargetGeneCreate(
        name=name,
        category=category,
        external_identifiers=external_identifiers,
        target_accession=target_accession,
    )
    assert externalIdentifier.name == "BRCA1"
    assert externalIdentifier.category == "regulatory"


def test_cannot_create_target_gene_with_invalid_category():
    name = "UBE2I"
    invalid_category = "invalid name"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dna", "sequence": SEQUENCE, "taxonomy": TEST_POPULATED_TAXONOMY}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=invalid_category,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert "Input should be 'protein_coding', 'regulatory' or 'other_noncoding'" in str(exc_info.value)


def test_cannot_create_target_gene_with_invalid_sequence_type():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dnaa", "sequence": SEQUENCE, "taxonomy": TEST_POPULATED_TAXONOMY}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"'{target_sequence['sequenceType']}' is not a valid sequence type" in str(exc_info.value)


def test_cannot_create_target_gene_with_mismatched_sequence_and_type():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dna", "sequence": "ARCG", "taxonomy": TEST_POPULATED_TAXONOMY}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"invalid {target_sequence['sequenceType']} sequence provided" in str(exc_info.value)


def test_cannot_create_target_gene_with_invalid_sequence():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dna", "sequence": "AOCG%", "taxonomy": TEST_POPULATED_TAXONOMY}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"invalid {target_sequence['sequenceType']} sequence provided" in str(exc_info.value)


def test_cannot_create_target_gene_without_sequence_or_accession():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
        )

    assert "Expected either a `target_sequence` or a `target_accession`, not neither." in str(exc_info.value)


def test_cannot_create_target_gene_with_both_sequence_and_accession():
    name = "UBE2I"
    category = "regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_accession = {"accession": "NM_001637.3", "assembly": "GRCh37", "gene": "BRCA1", "isBaseEditor": False}
    target_sequence = {
        "sequenceType": "dna",
        "sequence": SEQUENCE,
        "taxonomy": TEST_POPULATED_TAXONOMY,
    }
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
            target_accession=target_accession,
        )

    assert "Expected either a `target_sequence` or a `target_accession`, not both." in str(exc_info.value)


@pytest.mark.parametrize(
    "identifier",
    [
        {"key": "ensembl_offset", "value": TEST_ENSEMBLE_EXTERNAL_IDENTIFIER},
        {"key": "refseq_offset", "value": TEST_REFSEQ_EXTERNAL_IDENTIFIER},
        {"key": "uniprot_offset", "value": TEST_UNIPROT_EXTERNAL_IDENTIFIER},
    ],
)
def test_external_identifiers_parsed_into_containing_list(identifier):
    target_gene = {
        "id": 1,
        "name": "UBE2I",
        "category": "regulatory",
        "ensembl_offset": None,
        "refseq_offset": None,
        "uniprot_offset": None,
    }
    target_gene["target_sequence"] = dummy_attributed_object_from_dict(
        {
            "sequenceType": "dna",
            "sequence": SEQUENCE,
            "taxonomy": TEST_SAVED_TAXONOMY,
        }
    )

    # Tranform external identifier constant into an attributed object, with an added offset property
    external_identifier = identifier["key"]
    offset = 10
    target_gene[external_identifier] = {
        "offset": offset,
        "identifier": dummy_attributed_object_from_dict(identifier["value"]),
    }
    target_gene[external_identifier] = dummy_attributed_object_from_dict(target_gene[external_identifier])

    target_gene = dummy_attributed_object_from_dict(target_gene)
    saved_target_gene = SavedTargetGene.model_validate(target_gene)

    assert len(saved_target_gene.external_identifiers) == 1
    assert saved_target_gene.external_identifiers[0].identifier.identifier == identifier["value"]["identifier"]
    assert saved_target_gene.external_identifiers[0].identifier.db_name == identifier["value"]["db_name"]
    assert saved_target_gene.external_identifiers[0].offset == offset


def test_cannot_create_saved_target_without_seq_or_acc():
    target_gene = {
        "id": 1,
        "name": "UBE2I",
        "category": "regulatory",
        "ensembl_offset": None,
        "refseq_offset": None,
        "uniprot_offset": None,
    }

    target_gene = dummy_attributed_object_from_dict(target_gene)
    with pytest.raises(ValueError) as exc_info:
        SavedTargetGene.model_validate(target_gene)

    assert "Either a `target_sequence` or `target_accession` is required" in str(exc_info.value)
