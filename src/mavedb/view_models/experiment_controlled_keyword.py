from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models import keyword
from mavedb.lib.validation import keywords

from pydantic import root_validator
from typing import Optional


class ExperimentControlledKeywordBase(BaseModel):
    """Base class for experiment and controlled keyword bridge table view models."""
    keyword: keyword.KeywordBase
    description: Optional[str]

    @root_validator(pre=True)
    def validate_fields(cls, values):
        validated_keyword = values.get("keyword")
        validated_description = values.get("description")

        if validated_keyword:
            keywords.validate_description(validated_keyword['value'], validated_description)
        return values


class ExperimentControlledKeywordCreate(ExperimentControlledKeywordBase):
    """View model for creating a new keyword."""
    keyword: keyword.KeywordCreate


class ExperimentControlledKeywordUpdate(ExperimentControlledKeywordBase):
    """View model for updating a keyword."""
    pass


class SavedExperimentControlledKeyword(ExperimentControlledKeywordBase):
    """Base class for keyword view models representing saved records."""

    class Config:
        orm_mode = True


class ExperimentControlledKeyword(SavedExperimentControlledKeyword):
    """Keyword view model for non-admin clients."""
    pass
