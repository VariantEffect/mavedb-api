from mavedb.view_models.target_gene import TargetGeneCreate

import pytest


def test_create_target_gene_with_sequence():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_sequence = {
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
        "taxonomy": {
            "taxId": 9606,
            "organismName": "Homo sapiens",
            "commonName": "human",
            "rank": "SPECIES",
            "hasDescribedSpeciesName": True,
            "articleReference": "NCBI:txid9606",
            "genomeId": None,
            "id": 14,
            "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
        },
    }
    externalIdentifier = TargetGeneCreate(
        name=name,
        category=category,
        external_identifiers=external_identifiers,
        target_sequence=target_sequence,
    )
    assert externalIdentifier.name == "UBE2I"
    assert externalIdentifier.category == "Regulatory"


def test_create_target_gene_with_accession():
    name = "BRCA1"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_accession = {"accession": "NM_001637.3", "assembly": "GRCh37", "gene": "BRCA1"}
    externalIdentifier = TargetGeneCreate(
        name=name,
        category=category,
        external_identifiers=external_identifiers,
        target_accession=target_accession,
    )
    assert externalIdentifier.name == "BRCA1"
    assert externalIdentifier.category == "Regulatory"


def test_create_invalid_category():
    name = "UBE2I"
    invalid_category = "invalid name"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy = {
        "taxId": 9606,
        "organismName": "Homo sapiens",
        "commonName": "human",
        "rank": "SPECIES",
        "hasDescribedSpeciesName": True,
        "articleReference": "NCBI:txid9606",
        "genomeId": None,
        "id": 14,
        "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
    }
    target_sequence = {
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
            taxonomy=taxonomy,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert (
        "invalid name is not a valid target category. Valid categories are Protein coding, Regulatory, and Other"
        " noncoding" in str(exc_info.value)
    )


def test_create_invalid_sequence_type():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    taxonomy = {
        "taxId": 9606,
        "organismName": "Homo sapiens",
        "commonName": "human",
        "rank": "SPECIES",
        "hasDescribedSpeciesName": True,
        "articleReference": "NCBI:txid9606",
        "genomeId": None,
        "id": 14,
        "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
    }
    target_sequence = {
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
            taxonomy=taxonomy,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"'{target_sequence['sequenceType']}' is not a valid sequence type" in str(exc_info.value)


def test_create_not_match_sequence_and_type():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dna", "sequence": "ARCG"}
    taxonomy = {
        "taxId": 9606,
        "organismName": "Homo sapiens",
        "commonName": "human",
        "rank": "SPECIES",
        "hasDescribedSpeciesName": True,
        "articleReference": "NCBI:txid9606",
        "genomeId": None,
        "id": 14,
        "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
    }
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            taxonomy=taxonomy,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"invalid {target_sequence['sequenceType']} sequence provided" in str(exc_info.value)


def test_create_invalid_sequence():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 0}]
    target_sequence = {"sequenceType": "dna", "sequence": "AOCG%"}
    taxonomy = {
        "taxId": 9606,
        "organismName": "Homo sapiens",
        "commonName": "human",
        "rank": "SPECIES",
        "hasDescribedSpeciesName": True,
        "articleReference": "NCBI:txid9606",
        "genomeId": None,
        "id": 14,
        "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606",
    }
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            taxonomy=taxonomy,
            external_identifiers=external_identifiers,
            target_sequence=target_sequence,
        )
    assert f"invalid {target_sequence['sequenceType']} sequence provided" in str(exc_info.value)


def test_cant_create_target_gene_without_sequence_or_accession():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    with pytest.raises(ValueError) as exc_info:
        TargetGeneCreate(
            name=name,
            category=category,
            external_identifiers=external_identifiers,
        )

    assert "Expected either a `target_sequence` or a `target_accession`, not neither." in str(exc_info.value)


def test_cant_create_target_gene_with_both_sequence_and_accession():
    name = "UBE2I"
    category = "Regulatory"
    external_identifiers = [{"identifier": {"dbName": "Ensembl", "identifier": "ENSG00000103275"}, "offset": 1}]
    target_accession = {"accession": "NM_001637.3", "assembly": "GRCh37", "gene": "BRCA1"}
    target_sequence = {
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
        "taxonomy": {
            "taxId": 9606,
            "organismName": "Homo sapiens",
            "commonName": "human",
            "rank": "SPECIES",
            "hasDescribedSpeciesName": True,
            "articleReference": "NCBI:txid9606",
            "genomeId": None,
        },
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
