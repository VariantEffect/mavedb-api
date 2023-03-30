from mavedb.view_models.wild_type_sequence import WildTypeSequenceCreate

import pytest

def test_create_wild_type_sequence(test_empty_db):
    sequence_type = "protein"
    sequence = "ARCG"
    wildTypeSeq = WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert wildTypeSeq.sequence_type == "protein"
    assert wildTypeSeq.sequence == 'ARCG'

def test_create_invalid_sequence_type(test_empty_db):
    sequence_type = "dnaaa"
    sequence = "ATGAGTATTCAACATTTCCGTGTC"
    with pytest.raises(ValueError) as exc_info:
        WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)

def test_create_not_match_sequence_and_seq_type(test_empty_db):
    sequence_type = "dna"
    sequence = "ARCG"
    with pytest.raises(ValueError) as exc_info:
        WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert "invalid protein sequence provided" in str(exc_info.value)

def test_create_not_match_sequence_and_seq_type(test_empty_db):
    sequence_type = "protein"
    sequence = "AzCG"
    with pytest.raises(ValueError) as exc_info:
        WildTypeSequenceCreate(sequence_type=sequence_type, sequence=sequence)
    assert "invalid protein sequence provided" in str(exc_info.value)
