from typing import Optional, Literal

from pydantic import Field

from mavedb.view_models.base.base import BaseModel


class UserBase(BaseModel):
    """Base class for user view models."""

    username: str = Field(..., alias="orcidId")
    first_name: Optional[str]
    last_name: Optional[str]

    class Config:
        allow_population_by_field_name = True


class CurrentUserUpdate(BaseModel):
    """View model for updating the current user."""

    email: Optional[str]


class AdminUserUpdate(BaseModel):
    """View model for updating current user, for admin clients."""

    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    # TODO Change the type back to list[Literal["admin"]]. Currently this causes a startup error.
    # roles: list[Literal["admin"]]
    roles: list[str]


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
    roles: list[str]


class AdminUser(SavedUser):
    """User view model containing properties to return to admin clients."""

    id: int
    roles: list[str]
