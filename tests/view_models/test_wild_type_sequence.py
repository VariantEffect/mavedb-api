import datetime

import pytest

from mavedb.view_models.target_sequence import TargetSequenceCreate
from mavedb.view_models.taxonomy import TaxonomyCreate

taxonomy = TaxonomyCreate(
    id=1,
    code=1,
    organism_name="Organism",
    common_name="Common name",
    rank="Rank",
    has_described_species_name=False,
    url="url",
    article_reference="article_reference",
    creation_date=datetime.datetime.now(),
    modification_date=datetime.datetime.now(),
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
def test_create_wild_type_sequence(sequence_type, sequence):
    TargetSeq = TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy)
    assert TargetSeq.sequence_type == sequence_type.lower()
    assert TargetSeq.sequence == sequence.upper()


@pytest.mark.parametrize("sequence_type, sequence", [("dnaaa", "ATGAGTATTCAACATTTCCGTGTC"), ("null", "STARTREK")])
def test_create_invalid_sequence_type(sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy)
    assert f"'{sequence_type}' is not a valid sequence type" in str(exc_info.value)


@pytest.mark.parametrize("sequence_type, sequence", [("dna", "ARCG"), ("protein", "AzCG")])
def test_create_invalid_sequence(sequence_type, sequence):
    with pytest.raises(ValueError) as exc_info:
        TargetSequenceCreate(sequence_type=sequence_type, sequence=sequence, taxonomy=taxonomy)
    assert f"invalid {sequence_type} sequence provided" in str(exc_info.value)
