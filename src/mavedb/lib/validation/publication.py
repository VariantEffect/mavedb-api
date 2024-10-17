import datetime
from urllib.parse import urlparse

import idutils

from mavedb.lib.validation.constants.publication import valid_dbnames
from mavedb.lib.validation.exceptions import ValidationError


def validate_db_name(db_name: str):
    if db_name.strip() == "" or not db_name:
        raise ValidationError("db_name should not be empty")
    if db_name not in valid_dbnames:
        raise ValidationError(
            f"The `db_name` key within the identifier attribute of the external "
            f"publication identifier should take one of the following values: "
            f"{valid_dbnames}."
        )


def identifier_valid_for(identifier: str) -> dict[str, bool]:
    """
    Returns a list of db_names for which the provided identifier is
    a valid identifier.

    Parameters
    __________
    identifier : str
        The identifier to check

    Returns
    _______
    dict[str, bool]
        The db_names for which this identifier is a valid identifier
    """
    return {
        "PubMed": validate_pubmed(identifier),
        "bioRxiv": validate_biorxiv(identifier),
        "medRxiv": validate_medrxiv(identifier),
        "Crossref": idutils.is_doi(identifier) is not None,
    }


def infer_identifier_from_url(identifier: str) -> str:
    """
    Infers an identifier from a potential URL based on the database we believe the URL
    to be from.

    Parameters
    __________
    identifier : str
        The identifier / URL to parse

    Returns
    _______
    str
        The parsed identifier from the url or the original identifier.
    """
    url = urlparse(identifier)
    if url.netloc:
        # http://www.dx.doi.org/{DOI}
        if "dx.doi.org" in url.netloc:
            identifier = url.path.strip("/")

        # https://www.biorxiv.org/content/10.1101/2024.04.26.591310, # https://www.medrxiv.org/content/10.1101/2024.04.26.59131023
        elif "biorxiv.org" in url.netloc or "medrxiv.org" in url.netloc:
            identifier = url.path.strip("/").split("/")[-1]

        # https://pubmed.ncbi.nlm.nih.gov/24567513/, http://www.ncbi.nlm.nih.gov/pubmed/432
        elif "ncbi.nlm.nih.gov" in url.netloc:
            identifier = url.path.strip("/").split("/")[-1]

        # The url does not come from an accepted database.
        else:
            return identifier

    return identifier


def validate_publication(identifier: str) -> None:
    """
    Validates that a passed identifier is one we accept. Currently allowed
    publication identifiers:

    - PMIDs: Pubmed IDs are an 8 character integer (no leading zeros)
    - bioRxiv IDs: biorXiv IDs are either 6 digit identifiers if the
                   preprint article is from before 2019.12.11 (123456)
                   or the article publication date with a 6 digit
                   suffix if published after that date (2019.12.12.123456).
    - medRxiv IDs: medRxiv IDs are either 8 digit identifiers if the
                   preprint article is from before 2019.12.11 (12345678)
                   or the article publication date with a 8 digit
                   suffix if published after that date (2019.12.12.12345678).
    - valid DOIs: We use a RegEx provided by IDUtils to check this constraint.

    Note that preprint identifiers may have leading zeros, while PMIDs may not.

    Parameters
    __________
    identifier : str
        The identifier to be validated.

    Returns
    _______
    None
        If the identifier is in one of the formats we accept.
        NOTE: This does not imply that the identifier exists
        in the Crossref, PubMed, bioRxiv, or medRxiv databases.

    Raises
    ______
    ValidationError
        If the identifier is not an accepted publication identifier.
    """
    if not (
        validate_pubmed(identifier)
        or validate_biorxiv(identifier)
        or validate_medrxiv(identifier)
        or idutils.is_doi(identifier)
    ):
        raise ValidationError(f"'{identifier}' is not a valid DOI or a valid PubMed, bioRxiv, or medRxiv identifier.")


def validate_pubmed(identifier: str) -> bool:
    """
    Validates whether the provided identifier is a valid PubMed identifier.

    Parameters
    __________
    identifier : str
        The identifier to validate

    Returns
    _______
    bool
        Whether the identifier is valid.

    """
    return idutils.is_pmid(identifier) is not None


def validate_biorxiv(identifier: str) -> bool:
    """
    Validates whether the provided identifier is a valid biorxiv identifier.

    Parameters
    __________
    identifier : str
        The identifier to validate

    Returns
    _______
    bool
        Whether the identifier is valid.

    Raises
    ______
    ValueError (via _validate_new_preprint_format)
        If the provided date_parts cannot be coerced to a `date` object.
    """
    if "." in identifier:
        return _validate_new_preprint_format(identifier, 6)

    if identifier.isdigit():
        return _validate_old_preprint_format(identifier, 6)

    return False


def validate_medrxiv(identifier: str) -> bool:
    """
    Validates whether the provided identifier is a valid medrxiv identifier.

    Parameters
    __________
    identifier : str
        The identifier to validate.

    Returns
    _______
    bool
        Whether the identifier is valid.

    Raises
    ______
    ValueError (via _validate_new_preprint_format)
        If the provided date_parts cannot be coerced to a `date` object.
    """
    if "." in identifier:
        return _validate_new_preprint_format(identifier, 8)

    if identifier.isdigit():
        return _validate_old_preprint_format(identifier, 8)

    return False


def _validate_new_preprint_format(identifier: str, suffix_len: int) -> bool:
    """
    Validates whether the provided identifier is a valid pre-print identifier
    in the new format.

    Parameters
    __________
    identifier : str
        The identifier to validate.

    suffix_len : int
        The desired suffix length of the identifier.

    Returns
    _______
    bool
        Whether the identifier is valid.

    Raises
    ______
    ValueError (via _date_is_after_preprint_changeover)
        If the provided date_parts cannot be coerced to a `date` object.
    """
    preprint_id = identifier.split(".")
    preprint_suffix = preprint_id[-1]
    preprint_date = tuple(preprint_id[:-1])

    if not preprint_suffix.isdigit():
        return False

    if len(preprint_suffix) == suffix_len:
        return len(preprint_date) == 3 and _date_is_after_preprint_changeover(preprint_date)

    return False


def _validate_old_preprint_format(identifier: str, suffix_len: int) -> bool:
    """
    Validates that a passed identifier is an old pre-print (bioRxiv or medRxiv)
    identifier.

    Parameters
    __________
    identifier : str
        The identifier to validate.

    suffix_len : int
        The desired suffix length of the identifier.

    Returns
    _______
    bool
        Whether the identifier is valid.
    """
    return len(identifier) == suffix_len and identifier.isdigit()


def _date_is_after_preprint_changeover(date_parts: tuple[str, str, str]) -> bool:
    """
    Checks if a passed date with date parts comprised ot a three-tuple occurred
    after pre-print servers began using a new identifier format.

    Parameters
    __________
    date_parts : tuple[str, str, str]
        A tuple with three string entries (year, month, day) to check against
        the changeover date.

    Returns
    _______
    bool
        Whether the date occured after the pre-print server identifier changeover.

    Raises
    ______
    ValueError (via datetime.datetime.strptime)
        If the passed date_parts cannot be coerced to a `date` object.
    """
    date_cutoff = datetime.datetime.strptime("20191211", "%Y%m%d").date()
    date_part = datetime.datetime.strptime("".join(date_parts), "%Y%m%d").date()

    if date_part > date_cutoff:
        return True
    else:
        return False
