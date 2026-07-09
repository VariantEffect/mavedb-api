from typing import Literal, Union

from mavedb.lib.validation.identifier import validate_ensembl_identifier, validate_refseq_identifier


def infer_db_name_from_sequence_accession(
    sequence_accession: str,
) -> Union[Literal["RefSeq_Nucleotide", "RefSeq_Protein", "Ensembl_Protein", "Ensembl_Transcript"]]:
    """
    Infers the database name from a sequence accession.

    Args:
        sequence_accession (str): The sequence accession to analyze.

    Returns:
        str: The inferred database name.
    """
    if sequence_accession.startswith("ENSP"):
        validate_ensembl_identifier(sequence_accession)
        return "Ensembl_Protein"
    elif sequence_accession.startswith("ENST"):
        validate_ensembl_identifier(sequence_accession)
        return "Ensembl_Transcript"
    elif sequence_accession.startswith("NM_"):
        validate_refseq_identifier(sequence_accession)
        return "RefSeq_Nucleotide"
    elif sequence_accession.startswith("NP_"):
        validate_refseq_identifier(sequence_accession)
        return "RefSeq_Protein"

    raise NotImplementedError(
        "Only RefSeq (NM_/NP_) and Ensembl (ENSP/ENST) identifiers are currently supported for inference."
    )
