# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from typing import Optional

from pydantic import field_validator, model_validator

from mavedb.lib.validation import keywords
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class KeywordBase(BaseModel):
    """Base class for keyword view models.
    Keywords may have key but no value if users don't choose anything from dropdown menu.
    TODO#273: Controlled keywords are required once the final list is confirmed.
    """

    key: str
    label: Optional[str] = None
    system: Optional[str] = None
    code: Optional[str] = None
    version: Optional[str] = None
    special: Optional[bool] = None
    description: Optional[str] = None

    @field_validator("key")
    def validate_key(cls, v: str) -> str:
        keywords.validate_keyword(v)
        return v

    @model_validator(mode="after")
    def validate_code(self):
        keywords.validate_code(self.key, self.label, self.code)
        return self


    # TODO#273: Un-commenting this block will require new experiments to contain a keyword on creation.
    # @field_validator("label")
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
        from_attributes = True
        arbitrary_types_allowed = True


class Keyword(SavedKeyword):
    """Keyword view model for non-admin clients."""

    pass


class AdminKeyword(SavedKeyword):
    """Keyword view model containing properties to return to admin clients."""

    id: int
