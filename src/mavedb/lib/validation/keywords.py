from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null

from mavedb.models.controlled_keyword import ControlledKeyword


def find_keyword(db: Session, key: str, value: str):
    query = db.query(ControlledKeyword).filter(ControlledKeyword.key == key).filter(ControlledKeyword.value == value)
    controlled_keyword = query.one_or_none()
    if controlled_keyword is None:
        raise ValueError(f'Invalid keyword {key} or {value}')
    return controlled_keyword


def validate_description(value: str, description: Optional[str]):
    if value.lower() == "other" and (description is None or description.strip() == ""):
        raise HTTPException(status_code=403, detail="Other option does not allow empty description.")


def validate_duplicates(keywords: list):
    keys = []
    values = []
    for k in keywords:
        keys.append(k.keyword.key.lower())  # k: ExperimentControlledKeywordCreate object
        if k.keyword.value.lower() != "other":
            values.append(k.keyword.value.lower())

    keys_set = set(keys)
    values_set = set(values)

    if len(keys) != len(keys_set):
        raise HTTPException(status_code=403, detail="Duplicate keys found in keywords.")
    if len(values) != len(values_set):
        raise HTTPException(status_code=403, detail="Duplicate values found in keywords.")


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


def validate_keyword_keys(keywords: list):
    keys = []
    values = []
    for k in keywords:
        keys.append(k.keyword.key.lower())
        values.append(k.keyword.value.lower())

    if "endogenous locus library method" in values:
        if ("endogenous locus library method system" not in keys) or ("endogenous locus library method mechanism" not in keys):
            raise HTTPException(status_code=403, detail="Miss 'Endogenous Locus Library Method System' "
                                                        "or 'Endogenous Locus Library Method Mechanism' in keywords")
    elif "in vitro construct library method" in values:
        if ("in vitro construct library method system" not in keys) or ("in vitro construct library method mechanism" not in keys):
            raise HTTPException(status_code=403, detail="Miss 'In Vitro Construct Library Method System' "
                                                        "or 'In Vitro Construct Library Method Mechanism' in keywords")
    elif "other" in values:
        if ("endogenous locus library method system" in keys) or ("endogenous locus library method mechanism" in keys) or ("in vitro construct library method system" in keys) or ("in vitro construct library method mechanism" in keys):
            raise HTTPException(status_code=403, detail="Wrong keys in keywords.")


def validate_keyword_list(keywords: list):
    validate_duplicates(keywords)
    validate_keyword_keys(keywords)
