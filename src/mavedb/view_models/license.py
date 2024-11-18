from datetime import date
from typing import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class LicenseBase(BaseModel):
    """Base class for license view models."""

    long_name: str
    short_name: str
    active: bool
    link: Optional[str]
    version: Optional[str]


# Properties shared by models stored in DB
class SavedLicense(LicenseBase):
    """Base class for license view models representing saved records."""

    id: int
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


class ShortLicense(SavedLicense):
    """License view model containing a smaller set of properties to return in list contexts."""

    pass


# Properties to return to non-admin clients
class License(SavedLicense):
    """License view model containing properties visible to all users."""

    text: str
    creation_date: date
    modification_date: date
