from typing import Optional, Literal

from pydantic import Field

from src.view_models.base.base import BaseModel


class UserBase(BaseModel):
    username: str = Field(..., alias='orcidId')
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]

    class Config:
        allow_population_by_field_name = True


class UserUpdate(BaseModel):
    email: Optional[str]


class AdminUserUpdate(BaseModel):
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    roles: list[Literal['admin']]


# Properties shared by models stored in DB
class SavedUser(UserBase):
    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class User(SavedUser):
    roles: list[str]


# Properties to return to non-admin clients
class AdminUser(SavedUser):
    id: int
    roles: list[str]
