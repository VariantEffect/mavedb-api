from typing import Optional
from typing_extensions import Self

from pydantic import model_validator

from mavedb.lib.validation import keywords
from mavedb.view_models import keyword, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ExperimentControlledKeywordBase(BaseModel):
    """Base class for experiment and controlled keyword bridge table view models."""

    keyword: keyword.KeywordBase
    description: Optional[str] = None

    @model_validator(mode="after")
    def validate_fields(self) -> Self:
        # validated_keyword possible value: {'key': 'Delivery method', 'label': None}
        # Validate if keyword label is other, whether description is None.
        if self.keyword and self.keyword.label:
            keywords.validate_description(self.keyword.label, self.keyword.key, self.description)

        return self


class ExperimentControlledKeywordCreate(ExperimentControlledKeywordBase):
    """View model for creating a new keyword."""

    keyword: keyword.KeywordCreate


class ExperimentControlledKeywordUpdate(ExperimentControlledKeywordBase):
    """View model for updating a keyword."""

    pass


class SavedExperimentControlledKeyword(ExperimentControlledKeywordBase):
    """Base class for keyword view models representing saved records."""

    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


class ExperimentControlledKeyword(SavedExperimentControlledKeyword):
    """Keyword view model for non-admin clients."""

    pass
