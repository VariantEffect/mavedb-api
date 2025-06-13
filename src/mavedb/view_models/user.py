from typing import Optional

from email_validator import EmailNotValidError, validate_email
from pydantic import Field, field_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.user_role import UserRole
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class UserBase(BaseModel):
    """Base class for user view models."""

    username: str = Field(..., alias="orcidId")
    first_name: Optional[str] = None
    last_name: Optional[str] = None

    class Config:
        populate_by_name = True


class CurrentUserUpdate(BaseModel):
    """View model for updating the current user."""

    # TODO: Do we allow users to clear their emails?
    email: Optional[str] = None

    @field_validator("email")
    def validate_email_syntax_and_deliverability(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None

        try:
            normalized_email = validate_email(v, check_deliverability=True)
        except EmailNotValidError as exc:
            raise ValidationError(str(exc))

        return normalized_email.email


class AdminUserUpdate(CurrentUserUpdate):
    """View model for updating current user, for admin clients."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    roles: Optional[list[UserRole]] = None


class SavedUser(UserBase):
    """Base class for user view models representing saved records."""

    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


class User(SavedUser):
    """User view model containing properties visible to non-admin users."""

    pass


class CurrentUser(SavedUser):
    """User view model for information about the current user."""

    email: Optional[str] = None
    is_first_login: bool
    roles: list[UserRole]


class AdminUser(CurrentUser):
    """User view model containing properties to return to admin clients."""

    id: int
