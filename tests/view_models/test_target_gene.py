from mavedb.view_models.target_gene import TargetGeneCreate

import pytest


def test_create_target_gene(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    taxonomy_id = 1
    wt_sequence = {
        "sequenceType": "dna",
        "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGA"
                    "TGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAA"
                    "TGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAAT"
                    "GACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACAC"
                    "TGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGG"
                    "AACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTA"
                    "CTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTAT"
                    "TGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGA"
                    "CGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
    }
    externalIdentifier = TargetGeneCreate(
        name=name,
        category=category,
        taxonomy_id=taxonomy_id,
        external_identifiers=external_identifiers,
        wt_sequence=wt_sequence,
    )
    assert externalIdentifier.name == "UBE2I"
    assert externalIdentifier.category == "Regulatory"


def test_create_invalid_category(test_empty_db):
    name = "UBE2I"
    invalid_category = "invalid name"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy_id = 1
    wt_sequence = {
        "sequenceType": "dna",
        "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGA"
                    "TGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAA"
                    "TGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAAT"
                    "GACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACAC"
                    "TGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGG"
                    "AACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTA"
                    "CTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTAT"
                    "TGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGA"
                    "CGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
    }
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=invalid_category,
            taxonomy_id=taxonomy_id,
            external_identifiers=external_identifiers,
            wt_sequence=wt_sequence,
        )
    assert (
            "invalid name is not a valid target category. Valid categories are Protein coding, Regulatory, and Other"
            " noncoding"
            in str(exc_info.value)
    )


def test_create_invalid_sequence_type(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy_id = 1
    wt_sequence = {
        "sequenceType": "dnaa",
        "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGA"
                    "TGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAA"
                    "TGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAAT"
                    "GACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACAC"
                    "TGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGG"
                    "AACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTA"
                    "CTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTAT"
                    "TGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGA"
                    "CGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA",
    }
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            taxonomy_id=taxonomy_id,
            external_identifiers=external_identifiers,
            wt_sequence=wt_sequence,
        )
    assert f"'{wt_sequence['sequenceType']}' is not a valid sequence type" in str(exc_info.value)


def test_create_not_match_sequence_and_type(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy_id = 1
    wt_sequence = {"sequenceType": "dna", "sequence": "ARCG"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            taxonomy_id=taxonomy_id,
            external_identifiers=external_identifiers,
            wt_sequence=wt_sequence,
        )
    assert f"invalid {wt_sequence['sequenceType']} sequence provided" in str(exc_info.value)


def test_create_invalid_sequence(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy_id = 1
    wt_sequence = {"sequenceType": "dna", "sequence": "AOCG%"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            taxonomy_id=taxonomy_id,
            external_identifiers=external_identifiers,
            wt_sequence=wt_sequence,
        )
    assert f"invalid {wt_sequence['sequenceType']} sequence provided" in str(exc_info.value)
