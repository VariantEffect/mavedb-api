from datetime import date
from typing import Optional

from mavedb.view_models.base.base import BaseModel


class AccessKeyBase(BaseModel):
    key_id: str
    name: Optional[str]
    expiration_date: Optional[date]
    created_at: Optional[str]


# Properties shared by models stored in DB
class SavedAccessKey(AccessKeyBase):
    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class AccessKey(SavedAccessKey):
    pass


# Properties to return when an access key has just been created
class NewAccessKey(SavedAccessKey):
    private_key: str
