import pytest

from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession
from tests.helpers.constants import (
    VALID_CHR_ACCESSION,
    VALID_NT_ACCESSION,
    VALID_PRO_ACCESSION,
    VALID_UNIPROT_ACCESSION,
)


def test_infer_db_name_from_sequence_accession_nm():
    result = infer_db_name_from_sequence_accession(VALID_NT_ACCESSION)
    assert result == "RefSeq_Nucleotide"


def test_infer_db_name_from_sequence_accession_np():
    result = infer_db_name_from_sequence_accession(VALID_PRO_ACCESSION)
    assert result == "RefSeq_Protein"


# Both versioned and unversioned ENSP and ENST accessions must route to Ensembl_Protein and Ensembl_Transcript, respectively;
# the versioned case guards that validate_ensembl_identifier's version handling continues to accept it.
@pytest.mark.parametrize("ensembl_protein_accession", ["ENSP00000418960", "ENSP00000418960.3"])
def test_infer_db_name_from_sequence_accession_ensp(ensembl_protein_accession):
    result = infer_db_name_from_sequence_accession(ensembl_protein_accession)
    assert result == "Ensembl_Protein"


@pytest.mark.parametrize("ensembl_transcript_accession", ["ENST00000418960", "ENST00000418960.3"])
def test_infer_db_name_from_sequence_accession_enst(ensembl_transcript_accession):
    result = infer_db_name_from_sequence_accession(ensembl_transcript_accession)
    assert result == "Ensembl_Transcript"


@pytest.mark.parametrize("invalid_accession", ["XP_000000", VALID_CHR_ACCESSION])
def test_infer_db_name_from_sequence_accession_invalid(invalid_accession):
    with pytest.raises(NotImplementedError):
        infer_db_name_from_sequence_accession(invalid_accession)


def test_infer_db_name_from_non_refseq_accession():
    with pytest.raises(NotImplementedError):
        infer_db_name_from_sequence_accession(VALID_UNIPROT_ACCESSION)


def test_infer_db_name_from_sequence_accession_empty_string():
    with pytest.raises(NotImplementedError):
        infer_db_name_from_sequence_accession("")
