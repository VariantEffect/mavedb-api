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


def id_mapping_query_accession(sequence_accession: str, from_db: str) -> str:
    """
    Returns the form of a sequence accession to submit to UniProt ID mapping.

    UniProt's RefSeq_Nucleotide index matches versioned NM_ accessions exactly and lags RefSeq
    releases (e.g. it holds NM_002878.3 while RefSeq and the UniProtKB entry are at NM_002878.4),
    so current transcript versions often return no results. Unversioned NM_ accessions match
    reliably. The other supported databases match versioned accessions, so those pass through.

    Args:
        sequence_accession (str): The sequence accession to submit.
        from_db (str): The UniProt ID mapping database the accession belongs to.

    Returns:
        str: The accession to submit.
    """
    if from_db == "RefSeq_Nucleotide":
        return sequence_accession.split(".", 1)[0]

    return sequence_accession
