from typing import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ContributorBase(BaseModel):
    """Base class for contributor view models."""

    orcid_id: str


class ContributorCreate(ContributorBase):
    """View model for creating a new contributor or looking one up."""

    pass


class SavedContributor(ContributorBase):
    """Base class for contributor view models representing saved records."""

    record_type: str = None  # type: ignore
    given_name: Optional[str]
    family_name: Optional[str]

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


class Contributor(SavedContributor):
    """Contributor view model."""

    pass
