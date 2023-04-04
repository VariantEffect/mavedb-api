from mavedb.view_models.wild_type_sequence import WildTypeSequenceCreate

import pytest


@pytest.mark.parametrize(
    "sequence_type, sequence",
    [
        ("dna", "ATGAGTATTCAACATTTCCGTGTC"),
        ("dna", "ATGAGTATtcaatcTTTCCGTGTC"),
        ("protein", "STARTREK"),
        ("Protein", "startrek"),
    ],
)
def test_create_wild_type_sequence(test_empty_db, sequence_type, sequence):
    wildTypeSeq = WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert wildTypeSeq.sequence_type == sequence_type.lower()
    assert wildTypeSeq.sequence == sequence.upper()


@pytest.mark.parametrize("sequence_type, sequence", [("dnaaa", "ATGAGTATTCAACATTTCCGTGTC"), ("null", "STARTREK")])
def test_create_invalid_sequence_type(test_empty_db, sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)


@pytest.mark.parametrize("sequence_type, sequence", [("dna", "ARCG"), ("protein", "AzCG")])
def test_create_invalid_sequence(test_empty_db, sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert f"invalid {sequence_type} sequence provided" in str(exc_info.value)
