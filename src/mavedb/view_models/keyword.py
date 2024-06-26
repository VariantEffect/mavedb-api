# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations
from typing import Optional

from mavedb.lib.validation import keywords
from mavedb.view_models.base.base import BaseModel, validator


class KeywordBase(BaseModel):
    """Base class for keyword view models.
    Keywords may have key but no value if users don't choose anything from dropdown menu.
    Should modify it when we confirm the final controlled keyword list.
    """
    key: str
    value: Optional[str]
    vocabulary: Optional[str]
    special: Optional[bool]
    description: Optional[str]


class KeywordCreate(KeywordBase):
    """View model for creating a new keyword.
    Controlled keyword is optional now. We will change it in the future.
    validator("value") blocks creating a new experiment without controlled keywords so comment it first.
    """
    @validator("key")
    def validate_key(cls, v):
        keywords.validate_keyword(v)
        return v

    # @validator("value")
    # def validate_value(cls, v):
    #     keywords.validate_keyword(v)
    #     return v


class KeywordUpdate(KeywordBase):
    """View model for updating a keyword."""
    pass


class SavedKeyword(KeywordBase):
    """Base class for keyword view models representing saved records."""

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class Keyword(SavedKeyword):
    """Keyword view model for non-admin clients."""
    pass


class AdminKeyword(SavedKeyword):
    """Keyword view model containing properties to return to admin clients."""
    id: int