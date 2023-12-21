from mavedb.view_models.target_sequence import TargetSequenceCreate
from mavedb.models.reference_genome import ReferenceGenome

import pytest

import datetime

reference_genome = ReferenceGenome(
    id = 1,
    short_name = "Name",
    organism_name = "Organism",
    creation_date = datetime.datetime.now(),
    modification_date = datetime.datetime.now(),
)

@pytest.mark.parametrize(
    "sequence_type, sequence",
    [
        ("dna", "ATGAGTATTCAACATTTCCGTGTC"),
        ("dna", "ATGAGTATtcaatcTTTCCGTGTC"),
        ("protein", "STARTREK"),
        ("Protein", "startrek"),
    ],
)
def test_create_wild_type_sequence(client, sequence_type, sequence):
    TargetSeq = TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, reference=reference_genome)
    assert TargetSeq.sequence_type == sequence_type.lower()
    assert TargetSeq.sequence == sequence.upper()


@pytest.mark.parametrize("sequence_type, sequence", [("dnaaa", "ATGAGTATTCAACATTTCCGTGTC"), ("null", "STARTREK")])
def test_create_invalid_sequence_type(client, sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, reference=reference_genome)
    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)


@pytest.mark.parametrize("sequence_type, sequence", [("dna", "ARCG"), ("protein", "AzCG")])
def test_create_invalid_sequence(client, sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, reference=reference_genome)
    assert f"invalid {sequence_type} sequence provided" in str(exc_info.value)
