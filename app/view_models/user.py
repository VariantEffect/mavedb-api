from typing import Optional

from pydantic import Field

from app.view_models.base.base import BaseModel


class UserBase(BaseModel):
    username: str = Field(..., alias='orcid_id')
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]

    class Config:
        allow_population_by_field_name = True


class UserUpdate(BaseModel):
    email: Optional[str]


# Properties shared by models stored in DB
class SavedUser(UserBase):

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class User(SavedUser):
    pass
