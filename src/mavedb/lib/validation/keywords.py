from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null


def validate_keywords(keywords: list[str]):
    """
    Validates a list of keywords.

    Parameters
    __________
    keywords: list[str]
        A list of keywords.

    Raises
    ______
    ValidationError
        If the list is invalid or null or if any individual keyword is invalid or null.
    """
    if is_null(keywords):
        raise ValidationError(
            "{} are not valid keywords. Keywords must be a non null list of strings.".format(keywords)
        )
    else:
        for keyword in keywords:
            validate_keyword(keyword)


def validate_keyword(keyword: str):
    """
    This function validates whether or not the kw parameter is valid by
    checking that it is a string that is not null. If kw is null
    or is not a string, an error is raised.

    Parameters
    __________
    kw : str
        The keyword to be validated.

    Raises
    ______
    ValidationError
        If the kw argument is not a valid string.
    """
    if is_null(keyword) or not isinstance(keyword, str):
        raise ValidationError("{} not a valid keyword. Keywords must be non null strings.".format(keyword))
