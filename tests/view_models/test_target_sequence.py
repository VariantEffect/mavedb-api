from mavedb.view_models.target_sequence import TargetSequenceCreate

import pytest
import datetime


SEQUENCE = (
    "ATGAGTATTCAACATTTCCGTGTCGCCCTTATTCCCTTTTTTGCGGCATTTTGCCTTCCTGTTTTTGCTCACCCAGAAACGCTGGTGAAAGTAAAAGATGCT"
    "GAAGATCAGTTGGGTGCACGAGTGGGTTACATCGAACTGGATCTCAACAGCGGTAAGATCCTTGAGAGTTTTCGCCCCGAAGAACGTTTTCCAATGATGAGCACTTTTAAAGTTCT"
    "GCTATGTGGCGCGGTATTATCCCGTGTTGACGCCGGGCAAGAGCAACTCGGTCGCCGCATACACTATTCTCAGAATGACTTGGTTGAGTACTCACCAGTCACAGAAAAGCATCTTA"
    "CGGATGGCATGACAGTAAGAGAATTATGCAGTGCTGCCATAACCATGAGTGATAACACTGCGGCCAACTTACTTCTGACAACGATCGGAGGACCGAAGGAGCTAACCGCTTTTTTG"
    "CACAACATGGGGGATCATGTAACTCGCCTTGATCGTTGGGAACCGGAGCTGAATGAAGCCATACCAAACGACGAGCGTGACACCACGATGCCTGCAGCAATGGCAACAACGTTGCG"
    "CAAACTATTAACTGGCGAACTACTTACTCTAGCTTCCCGGCAACAATTAATAGACTGGATGGAGGCGGATAAAGTTGCAGGACCACTTCTGCGCTCGGCCCTTCCGGCTGGCTGGT"
    "TTATTGCTGATAAATCTGGAGCCGGTGAGCGTGGGTCTCGCGGTATCATTGCAGCACTGGGGCCAGATGGTAAGCCCTCCCGTATCGTAGTTATCTACACGACGGGGAGTCAGGCA"
    "ACTATGGATGAACGAAATAGACAGATCGCTGAGATAGGTGCCTCACTGATTAAGCATTGGTAA"
)

TAXONOMY = {
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


def test_create_valid_target_sequence():
    sequence_type = "dna"
    label = "sequence_label"
    sequence = SEQUENCE
    taxonomy = TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE
    assert target_sequence.label == label


def test_cannot_create_target_sequence_with_label_containing_colon():
    sequence_type = "dna"
    label = "sequence:label"
    sequence = SEQUENCE
    taxonomy = TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        target_sequence = TargetSequenceCreate(
            sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
        )

    assert f"Target sequence label `{label}` may not contain a colon." in str(exc_info.value)


def test_cannot_create_target_sequence_with_invalid_sequence_type():
    sequence_type = "invalid"
    label = "sequence_label"
    sequence = SEQUENCE
    taxonomy = TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        target_sequence = TargetSequenceCreate(
            sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
        )

    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)


def test_cannot_create_target_sequence_with_invalid_inferred_type():
    sequence_type = "infer"
    label = "sequence_label"
    sequence = SEQUENCE + "!"
    taxonomy = TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        target_sequence = TargetSequenceCreate(
            sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
        )

    assert "sequence is invalid" in str(exc_info.value)


@pytest.mark.parametrize(
    "sequence_type,exc_string",
    (
        ("dna", "invalid dna sequence provided"),
        ("protein", "invalid protein sequence provided"),
        ("invalid", "'invalid' is not a valid sequence type"),
    ),
)
def test_cannot_create_target_sequence_with_invalid_sequence(sequence_type, exc_string):
    label = "sequence_label"
    sequence = SEQUENCE + "!"
    taxonomy = TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        target_sequence = TargetSequenceCreate(
            sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
        )

    assert exc_string in str(exc_info.value)
