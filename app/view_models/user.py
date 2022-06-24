from pydantic import Field

from app.view_models.base.base import BaseModel
from pydantic.types import Optional


class UserBase(BaseModel):
    username: str = Field(..., alias='orcid_id')
    first_name: Optional[str]
    last_name: Optional[str]
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
