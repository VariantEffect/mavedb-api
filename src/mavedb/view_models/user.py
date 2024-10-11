from typing import Optional

from pydantic import Field
from email_validator import validate_email, EmailNotValidError

from mavedb.models.enums.user_role import UserRole
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models.base.base import BaseModel, validator


class UserBase(BaseModel):
    """Base class for user view models."""

    username: str = Field(..., alias="orcidId")
    first_name: Optional[str]
    last_name: Optional[str]

    class Config:
        allow_population_by_field_name = True


class CurrentUserUpdate(BaseModel):
    """View model for updating the current user."""

    # TODO: Do we allow users to clear their emails?
    email: Optional[str]

    @validator("email")
    def validate_email_syntax_and_deliverability(cls, field_value, values):
        if not field_value:
            return None

        try:
            normalized_email = validate_email(field_value, check_deliverability=True)
        except EmailNotValidError as exc:
            raise ValidationError(str(exc))

        return normalized_email.email


class AdminUserUpdate(CurrentUserUpdate):
    """View model for updating current user, for admin clients."""

    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    roles: Optional[list[UserRole]]


class SavedUser(UserBase):
    """Base class for user view models representing saved records."""

    class Config:
        orm_mode = True


class User(SavedUser):
    """User view model containing properties visible to non-admin users."""

    pass


class CurrentUser(SavedUser):
    """User view model for information about the current user."""

    email: Optional[str]
    is_first_login: bool
    roles: list[UserRole]


class AdminUser(CurrentUser):
    """User view model containing properties to return to admin clients."""

    id: int
