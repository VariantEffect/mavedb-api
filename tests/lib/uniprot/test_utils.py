import pytest

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession

from tests.helpers.constants import (
    VALID_NT_ACCESSION,
    VALID_PRO_ACCESSION,
    VALID_CHR_ACCESSION,
    VALID_UNIPROT_ACCESSION,
)


def test_infer_db_name_from_sequence_accession_nm():
    result = infer_db_name_from_sequence_accession(VALID_NT_ACCESSION)
    assert result == "RefSeq_Nucleotide"


def test_infer_db_name_from_sequence_accession_np():
    result = infer_db_name_from_sequence_accession(VALID_PRO_ACCESSION)
    assert result == "RefSeq_Protein"


@pytest.mark.parametrize("invalid_accession", ["XP_000000", VALID_CHR_ACCESSION])
def test_infer_db_name_from_sequence_accession_invalid(invalid_accession):
    with pytest.raises(NotImplementedError):
        infer_db_name_from_sequence_accession(invalid_accession)


def test_infer_db_name_from_non_refseq_accession():
    with pytest.raises(ValidationError):
        infer_db_name_from_sequence_accession(VALID_UNIPROT_ACCESSION)


def test_infer_db_name_from_sequence_accession_empty_string():
    with pytest.raises(ValidationError):
        infer_db_name_from_sequence_accession("")
