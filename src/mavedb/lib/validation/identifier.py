import idutils

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null
from mavedb.lib.validation.constants.identifier import valid_dbnames


def validate_db_name(db_name: str):
    if db_name.strip() == "" or not db_name:
        raise ValidationError("db_name should not be empty")
    if db_name not in valid_dbnames:
        raise ValidationError(
            f"The `db_name` key within the identifier attribute of the external identifier should "
            f"take one of the following values: {valid_dbnames}."
        )


def validate_identifier(db_name: str, identifier: str):
    if db_name == "UniProt":
        validate_uniprot_identifier(identifier)
    elif db_name == "RefSeq":
        validate_refseq_identifier(identifier)
    elif db_name == "Ensembl":
        validate_ensembl_identifier(identifier)


def validate_sra_identifier(identifier: str):
    """
    Validates whether the identifier is a valid SRA identifier.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Raises
    ______
    ValidationError
        If the identifier is not a valid SRA identifier.
    """
    if not (
        idutils.is_sra(identifier)
        or idutils.is_bioproject(identifier)
        or idutils.is_geo(identifier)
        or idutils.is_arrayexpress_array(identifier)
        or idutils.is_arrayexpress_experiment(identifier)
    ):
        raise ValidationError(f"'{identifier} is not a valid SRA, GEO, ArrayExpress or BioProject " "accession.")


def validate_ensembl_identifier(identifier: str):
    """
    Validates whether the identifier is a valid Ensembl identifier.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Raises
    ______
    ValidationError
        If the identifier is not a valid Ensembl identifier.
    """
    if not idutils.is_ensembl(identifier):
        raise ValidationError(f"'{identifier}' is not a valid Ensembl accession.")


def validate_uniprot_identifier(identifier: str):
    """
    Validates whether the identifier is a valid UniProt identifier.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Raises
    ______
    ValidationError
        If the identifier is not a valid UniProt identifier.
    """
    if not idutils.is_uniprot(identifier):
        raise ValidationError(f"'{identifier}' is not a valid UniProt accession.")


def validate_refseq_identifier(identifier: str):
    """
    Validates whether the identifier is a valid RefSeq identifier.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Raises
    ______
    ValidationError
        If the identifier is not a valid RefSeq identifier.
    """
    if not idutils.is_refseq(identifier):
        raise ValidationError(f"'{identifier}' is not a valid RefSeq accession.")


def validate_genome_identifier(identifier: str):
    """
    Validates whether the identifier is a valid genome identifier.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Raises
    ______
    ValidationError
        If the identifier is not a valid genome identifier.
    """
    if not idutils.is_genome(identifier):
        raise ValidationError(f"'{identifier}' is not a valid GenBank or RefSeq genome assembly.")


def validate_sra_list(values: list[str]):
    """
    Validates whether each identifier in a list of identifiers (values) is a valid SRA identifier.

    Parameters
    __________
    identifier : list[str]
        The list of identifiers to be validated.

    Raises
    ______
    ValidationError
        If at least one of the identifiers is not a valid SRA identifier.
    """
    for value in values:
        if not is_null(value):
            validate_sra_identifier(value)


def validate_ensembl_list(values: list[str]):
    """
    Validates whether each identifier in a list of identifiers (values) is a valid Ensembl identifier.

    Parameters
    __________
    identifier : list[str]
        The list of identifiers to be validated.

    Raises
    ______
    ValidationError
        If at least one of the identifiers is not a valid Ensemble identifier.
    """
    for value in values:
        if not is_null(value):
            validate_ensembl_identifier(value)


def validate_refseq_list(values: list[str]):
    """
    Validates whether each identifier in a list of identifiers (values) is a valid RefSeq identifier.

    Parameters
    __________
    identifier : list[str]
        The list of identifiers to be validated.

    Raises
    ______
    ValidationError
        If at least one of the identifiers is not a valid RefSeq identifier.
    """
    for value in values:
        if not is_null(value):
            validate_refseq_identifier(value)


def validate_uniprot_list(values: list[str]):
    """
    Validates whether each identifer in a list of identifiers (values) is a valid UniProt identifier.

    Parameters
    __________
    identifier : list[str]
        The list of identifiers to be validated.

    Raises
    ______
    ValidationError
        If at least one of the identifiers is not a valid UniProt identifier.
    """
    for value in values:
        if not is_null(value):
            validate_uniprot_identifier(value)
