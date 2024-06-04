from sqlalchemy.orm import Session

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null

from mavedb.models.controlled_keyword import ControlledKeyword


def validate_keyword(keyword: str):
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
    if is_null(keyword) or not isinstance(keyword, str):
        raise ValidationError(
            "{} are not valid keywords. Keywords must be a non null list of strings.".format(keyword)
        )


def find_keyword(db: Session, key: str, value: str):
    query = db.query(ControlledKeyword).filter(ControlledKeyword.key == key).filter(ControlledKeyword.value == value)
    controlled_keyword = query.one_or_none()
    if controlled_keyword is None:
        raise ValueError(f'Invalid keyword {key} or {value}')
    return controlled_keyword
