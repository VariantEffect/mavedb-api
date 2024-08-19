from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional

from mavedb.models.controlled_keyword import ControlledKeyword


def search_keyword(db: Session, key: str, value: Optional[str]):
    lower_key = key.lower().strip()
    lower_value = value.lower().strip()
    query = db.query(ControlledKeyword)
    if lower_key:
        query = query.filter(func.lower(ControlledKeyword.key) == lower_key)
    if lower_value:
        query = query.filter(func.lower(ControlledKeyword.value) == lower_value)

    controlled_keyword = query.one_or_none()
    if controlled_keyword is None:
        raise ValueError(f'Invalid keyword {key} or {value}')
    return controlled_keyword



