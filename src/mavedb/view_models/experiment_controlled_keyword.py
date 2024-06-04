from typing import Optional

from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models import keyword


class ExperimentControlledKeywordBase(BaseModel):
    """Base class for experiment and controlled keyword bridge table view models."""
    keyword: keyword.KeywordBase
    description: Optional[str]


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
