import pytest

from mavedb.view_models.target_sequence import TargetSequenceCreate, sanitize_target_sequence_label
from tests.helpers.constants import TEST_POPULATED_TAXONOMY

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


def test_create_valid_target_sequence():
    sequence_type = "dna"
    label = "sequence_label"
    sequence = SEQUENCE
    taxonomy = TEST_POPULATED_TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE
    assert target_sequence.label == label


def test_create_target_sequence_with_carriage_return():
    sequence_type = "dna"
    label = "sequence_label"
    sequence = SEQUENCE + "AGG\rATCG\r"
    taxonomy = TEST_POPULATED_TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE + "AGGATCG"
    assert target_sequence.label == label


def test_create_target_sequence_with_new_line():
    sequence_type = "dna"
    label = "sequence_label"
    sequence = SEQUENCE + "\nATCG\n"
    taxonomy = TEST_POPULATED_TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE + "ATCG"
    assert target_sequence.label == label


def test_target_sequence_label_is_sanitized():
    sequence_type = "dna"
    label = "   sanitize this label      "
    sequence = SEQUENCE
    taxonomy = TEST_POPULATED_TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE
    assert target_sequence.label == sanitize_target_sequence_label(label)


def test_target_sequence_label_can_be_nonetype():
    sequence_type = "dna"
    label = None
    sequence = SEQUENCE
    taxonomy = TEST_POPULATED_TAXONOMY

    target_sequence = TargetSequenceCreate(
        sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label
    )

    assert target_sequence.sequence_type == sequence_type
    assert target_sequence.sequence == SEQUENCE
    assert target_sequence.label is None


def test_cannot_create_target_sequence_with_label_containing_colon():
    sequence_type = "dna"
    label = "sequence:label"
    sequence = SEQUENCE
    taxonomy = TEST_POPULATED_TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label)

    assert f"Target sequence label `{label}` may not contain a colon." in str(exc_info.value)


def test_cannot_create_target_sequence_with_invalid_sequence_type():
    sequence_type = "invalid"
    label = "sequence_label"
    sequence = SEQUENCE
    taxonomy = TEST_POPULATED_TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label)

    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)


def test_cannot_create_target_sequence_with_invalid_inferred_type():
    sequence_type = "infer"
    label = "sequence_label"
    sequence = SEQUENCE + "!"
    taxonomy = TEST_POPULATED_TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label)

    assert "invalid inferred sequence type" in str(exc_info.value)


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
    taxonomy = TEST_POPULATED_TAXONOMY

    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy, label=label)

    assert exc_string in str(exc_info.value)
