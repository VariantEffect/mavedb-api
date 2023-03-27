from mavedb.view_models.target_gene import TargetGeneCreate

import pytest

def test_create_target_gene(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [
                {"identifier":
                    {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}
                ]
    reference_maps = [{"genomeId":1}]
    wt_sequence = {"sequenceType": "dna", "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA"}
    externalIdentifier = TargetGeneCreate(name=name, category=category, reference_maps=reference_maps, external_identifiers=external_identifiers, wt_sequence=wt_sequence)
    assert externalIdentifier.name == "UBE2I"
    assert externalIdentifier.category == 'Regulatory'

def test_create_invalid_category(test_empty_db):
    name = "UBE2I"
    invalid_category = "invalid name"
    external_identifiers = [
        {"identifier":
             {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}
    ]
    reference_maps = [{"genomeId": 1}]
    wt_sequence = {"sequenceType": "dna",
                   "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(name=name, category=invalid_category, reference_maps=reference_maps,
                         external_identifiers=external_identifiers, wt_sequence=wt_sequence)
    assert "invalid name is not a valid target category. Valid categories are Protein coding, Regulatory, and Other noncoding" in str(exc_info.value)

def test_create_invalid_sequence_type(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [
        {"identifier":
             {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}
    ]
    reference_maps = [{"genomeId": 1}]
    wt_sequence = {"sequenceType": "dnaa",
                   "sequence": "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCTGAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCTGCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTACGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTGCACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCGCAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGTTTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCAACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(name=name, category=category, reference_maps=reference_maps,
                         external_identifiers=external_identifiers, wt_sequence=wt_sequence)
    assert "sequence_type is invalid." in str(exc_info.value)

def test_create_not_match_sequence_and_type(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [
        {"identifier":
             {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}
    ]
    reference_maps = [{"genomeId": 1}]
    wt_sequence = {"sequenceType": "dna",
                   "sequence": "ARCG"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(name=name, category=category, reference_maps=reference_maps,
                         external_identifiers=external_identifiers, wt_sequence=wt_sequence)
    assert "sequence type does not match sequence_type. sequence type is protein, while sequence_type is dna." in str(exc_info.value)

def test_create_invalid_sequence(test_empty_db):
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [
        {"identifier":
             {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}
    ]
    reference_maps = [{"genomeId": 1}]
    wt_sequence = {"sequenceType": "dna",
                   "sequence": "AOCG%"}
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(name=name, category=category, reference_maps=reference_maps,
                         external_identifiers=external_identifiers, wt_sequence=wt_sequence)
    assert "sequence is invalid. It is not a correct target sequence." in str(exc_info.value)