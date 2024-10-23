# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from typing import Optional

from mavedb.lib.validation import keywords
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator


class KeywordBase(BaseModel):
    """Base class for keyword view models.
    Keywords may have key but no value if users don't choose anything from dropdown menu.
    TODO: Should modify it when we confirm the final controlled keyword list.
    """

    key: str
    value: Optional[str]
    vocabulary: Optional[str]
    special: Optional[bool]
    description: Optional[str]

    @validator("key")
    def validate_key(cls, v):
        keywords.validate_keyword(v)
        return v

    # validator("value") blocks creating a new experiment without controlled keywords so comment it first.
    # @validator("value")
    # def validate_value(cls, v):
    #     keywords.validate_keyword(v)
    #     return v


class KeywordCreate(KeywordBase):
    """View model for creating a new keyword."""

    pass


class KeywordUpdate(KeywordBase):
    """View model for updating a keyword."""

    pass


class SavedKeyword(KeywordBase):
    """Base class for keyword view models representing saved records."""

    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class Keyword(SavedKeyword):
    """Keyword view model for non-admin clients."""

    pass


class AdminKeyword(SavedKeyword):
    """Keyword view model containing properties to return to admin clients."""

    id: int
