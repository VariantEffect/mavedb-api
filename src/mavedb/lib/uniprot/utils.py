from typing import Literal, Union

from mavedb.lib.validation.identifier import validate_refseq_identifier


def infer_db_name_from_sequence_accession(
    sequence_accession: str,
) -> Union[Literal["RefSeq_Nucleotide", "RefSeq_Protein"]]:
    """
    Infers the database name from a sequence accession.

    Args:
        sequence_accession (str): The sequence accession to analyze.

    Returns:
        str: The inferred database name.
    """
    validate_refseq_identifier(sequence_accession)

    if sequence_accession.startswith("NM_"):
        return "RefSeq_Nucleotide"
    if sequence_accession.startswith("NP_"):
        return "RefSeq_Protein"

    raise NotImplementedError("Only RefSeq NM and NP identifiers are currently supported for inference.")
