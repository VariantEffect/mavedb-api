from datetime import date
from typing import Optional

from mavedb.models.enums.user_role import UserRole
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class AccessKeyBase(BaseModel):
    key_id: str
    name: Optional[str]
    expiration_date: Optional[date]
    created_at: Optional[str]


# Properties shared by models stored in DB
class SavedAccessKey(AccessKeyBase):
    record_type: str = None  # type: ignore
    role: Optional[UserRole]

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class AccessKey(SavedAccessKey):
    pass


# Properties to return when an access key has just been created
class NewAccessKey(SavedAccessKey):
    private_key: str
