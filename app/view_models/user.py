from pydantic import Field

from app.view_models.base.base import BaseModel


class UserBase(BaseModel):
    username: str = Field(..., alias='orcid_id')

    class Config:
        allow_population_by_field_name = True


# Properties shared by models stored in DB
class SavedUser(UserBase):
    pass

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class User(SavedUser):
    pass
